#!/usr/bin/env ruby
ENV['TZ'] = 'Asia/Tokyo'
require 'bundler/setup'
require 'logger'
require 'pp'
require 'yaml'
require 'time'
require 'uri'
require 'open-uri'
require 'nokogiri'
require 'rack/utils'
require 'fluent-logger'
require 'aws-sdk'
require 'json'
require 'fileutils'
require 'socket'

$stdout.sync = true

module Aandg
  class S3Cleaner
    def initialize(config, hostname = nil)
      @hostname = (hostname || ENV['AGQR_HOSTNAME'] || Socket.gethostname)
      @s3_region = ENV['AGQR_S3_REGION'] || config['s3_region']
      @s3_bucket = ENV['AGQR_S3_BUCKET'] || config['s3_bucket']
      @s3_prefix = (ENV['AGQR_S3_PREFIX'] || config['s3_prefix'] || '').sub(/\/\z/,'')
      @s3_access_key_id = config['aws_access_key_id']
      @s3_secret_access_key =  config['aws_secret_access_key']
      @url_base = ENV['AGQR_URL_BASE'] || config['http_base'] || "http://localhost"

      raise ArgumentError unless @s3_region && @s3_bucket
    end

    attr_reader :hostname, :s3_region, :s3_bucket, :s3_prefix, :url_base

    def s3
      @s3 ||= begin
        if @s3_access_key_id && @s3_secret_access_key
          Aws::S3::Client.new(region: @s3_region, credentials: Aws::Credentials.new(@s3_access_key_id, @s3_secret_access_key), logger: Logger.new($stdout))
        else
          Aws::S3::Client.new(region: @s3_region, logger: Logger.new($stdout))
        end
      end
    end

    def program_prefixes 
      @program_prefixes ||= s3.list_objects(bucket: s3_bucket, prefix: "#{s3_prefix}/", delimiter: '/').flat_map(&:common_prefixes).map do |x|
        ProgramPrefix.new(s3, s3_bucket, x.prefix, hostname)
      end
      # cp.prefix.sub(/\A#{Regexp.escape("#{S3_PREFIX}/")}/, '').sub(%r{/\z}, '')
    end

    def existing_work_prefixes
      program_prefixes.flat_map(&:work_prefixes)
    end

    def run!
      updated_program_prefixes = []

      existing_work_prefixes.each do |work_prefix|
        ran = run_on_work_prefix(work_prefix)
        if ran
          updated_program_prefixes << work_prefix.program_prefix
        end
      end

      updated_program_prefixes.each do |program_prefix|
        program_prefix.recordings(:reload)
        update_index program_prefix
        update_feed program_prefix
      end
    end

    def update_index(program_prefix)
      key = "#{program_prefix.prefix}index.html"
      existing_index = begin
        s3.get_object(bucket: s3_bucket, key: key).body.read
      rescue Aws::S3::Errors::NoSuchKey
        "<ul>\n"
      end
      new_index = existing_index.dup
      existing_index_html = Nokogiri::HTML(existing_index)

      missing_recordings = program_prefix.recordings.reject do |x|
        existing_index_html.at("li[data-agqr-pubdate-str='#{x.pubdate_str}']")
      end

      missing_recordings.sort_by!(&:pubdate)

      missing_recordings.each do |x|
        new_index << "<li data-agqr-pubdate-str='#{Rack::Utils.escape_html(x.pubdate_str)}' data-agqr-meta=\"#{Rack::Utils.escape_html(x.metadata.to_json)}\">"
        new_index << "<a href='/#{x.meta.key}'>#{x.pubdate_str}</a>: "
        if x.metadata['program']
          new_index << "#{Rack::Utils.escape_html(x.metadata['program']['title'])} - #{x.metadata['program']['personality']}"
        end
        new_index << " <a href='/#{x.mp3.key}'>mp3</a>"
        new_index << " <a href='/#{x.mp4.key}'>mp4</a>"
        new_index << "</li>\n"
      end

      s3.put_object(
        bucket: s3_bucket,
        key: key,
        body: new_index,
        content_type: 'text/html',
      )
    end

    def update_feed(program_prefix)
    end

    def run_on_work_prefix(work_prefix)
      puts "=> #{work_prefix.prefix}"
      unless work_prefix.target?
        puts " * Not a target, skip"
        return false
      end

      if work_prefix.host_works.empty?
        puts " * WARN: no host works, skip"
        return false
      end

      worker = work_prefix.worker
      if worker && worker != hostname
        puts " * #{worker.inspect} is working on, skipping"
        return false
      elsif !worker
        unless work_prefix.won_vote?
          puts " * #{work_prefix.vote_winner.host.inspect} won a vote, skipping"
          return false
        end
      end

      program_prefix = work_prefix.program_prefix

      puts " * Declearing work"
      work_prefix.declare_work!

      meta_prefix = "#{program_prefix.prefix}#{work_prefix.pubdate_str}.json"
      begin
        meta_obj = s3.head_object(bucket: s3_bucket, key: meta_prefix)
        if meta_obj
          puts " * WARN: meta json already exists! skipping"
          return false
        end
      rescue Aws::S3::Errors::NoSuchKey, Aws::S3::Errors::NotFound
      end

      best_work = work_prefix.best_work
      puts " * best work is #{best_work.prefix.inspect}"

      puts " * Extracting best mp3 & mp4 to #{program_prefix.prefix.inspect}"
      best_work.extract_works("#{program_prefix.prefix}rec/")

      rec_prefix = "#{program_prefix.prefix}rec/#{work_prefix.pubdate_str}/"

      if best_work.error_count > 0
        puts " * best work's error count is #{best_work.error_count}, keeping other host's work"
        work_prefix.host_works.each do |host_work|
          new_prefix = "#{rec_prefix}#{host_work.host}/"
          puts " - #{host_work.prefix.inspect} => #{new_prefix.inspect}"
          host_work.move_to!(new_prefix)
        end
      else
        puts " * removing host works"
        work_prefix.host_works.each do |x|
          if x == best_work
            new_prefix = "#{rec_prefix}#{x.host}/"
            x.move_to!(new_prefix, skip_single: true)
          else
            x.destroy!
          end
        end
      end

      true
    ensure
      work_prefix.declare_work_finish!
    end

    class ProgramPrefix
      def initialize(s3, bucket, prefix, hostname)
        @s3 = s3
        @bucket = bucket
        @prefix = prefix
        @hostname = hostname
      end

      attr_reader :bucket, :prefix

      def work_prefixes
        @work_prefixes ||= @s3.list_objects(bucket: bucket, prefix: "#{prefix}work/", delimiter: '/').flat_map(&:common_prefixes).map do |x|
          WorkPrefix.new(@s3, bucket, x.prefix, @hostname, self)
        end
      end

      def recordings(reload = false)
        @recordings = nil if reload
        @recordings ||= begin
          rec_prefix = "#{prefix}rec/"
          @s3.list_objects(bucket: bucket, prefix: rec_prefix, delimiter: '/').flat_map(&:contents).group_by do |content|
            content.key[rec_prefix.size..-1].split(?/,2).first.sub(%r{\..+\z},'')
          end.map do |pubdate_str, contents|
            Recording.new(@s3, bucket, pubdate_str, contents)
          end
        end
      end
    end

    class Recording
      def initialize(s3, bucket, pubdate_str, contents)
        @s3 = s3
        @bucket = bucket
        @pubdate_str = pubdate_str
        @contents = contents
      end

      attr_reader :bucket, :pubdate_str, :contents

      def pubdate
        @pubdate ||= Time.strptime(pubdate_str, '%Y-%m-%d_%H%M%S')
      end

      def mp4
        @mp4 ||= contents.find { |_| _.key.end_with? "rec/#{pubdate_str}.mp4" }
      end

      def mp3
        @mp3 ||= contents.find { |_| _.key.end_with? "rec/#{pubdate_str}.mp3" }
      end

      def meta
        @meta ||= contents.find { |_| _.key.end_with? "rec/#{pubdate_str}.json" }
      end

      def metadata
        @metadata ||= meta && JSON.parse(@s3.get_object(bucket: bucket, key: meta.key).body.read)
      end
    end

    class WorkPrefix
      def initialize(s3, bucket, prefix, hostname, program_prefix)
        @s3 = s3
        @bucket = bucket
        @prefix = prefix
        @hostname = hostname
        @program_prefix = program_prefix
      end

      attr_reader :bucket, :prefix, :program_prefix

      def pubdate_str
        @pubdate_str ||= prefix.split(?/)[-1]
      end

      def pubdate
        @pubdate ||= Time.strptime(pubdate_str, '%Y-%m-%d_%H%M%S')
      end

      def host_works
        @host_works ||= @s3.list_objects(bucket: bucket, prefix: prefix).flat_map(&:contents).group_by do |content|
          content.key[prefix.size..-1].split(?/,2).first
        end.map do |host, contents|
          HostWork.new(@s3, bucket, contents.first.key.sub(%r{\A(.+)/.+?\z},'\1').concat(?/), contents, pubdate_str)
        end
      end

      def target?
        return true if ENV['AGQR_DEBUG'] == '1'
        finished_work = host_works.select do |work|
          begin
            work.meta && true
          rescue Aws::S3::Errors::NoSuchKey
            false
          end
        end.min_by(&:meta_modified_at)

        (Time.now - finished_work.meta_modified_at) > (60 * 5)
      end

      def best_work
        host_works.sort_by(&:error_count).first
      end

      def vote_winner
        host_works.reject{ |_| _.vote_value < 0 }.sort_by { |_| [_.vote_value, _.host] }.last
      end

      def won_vote?
        vote_winner && @hostname == vote_winner.host
      end

      def worker
        @s3.get_object(
          bucket: bucket,
          key: "#{prefix}work-mark",
        ).body.read.chomp
      rescue Aws::S3::Errors::NoSuchKey
        nil
      end

      def declare_work!
        @s3.put_object(
          bucket: bucket,
          key: "#{prefix}work-mark",
          body: @hostname,
          content_type: 'text/plain',
        )
      end

      def declare_work_finish!
        @s3.delete_object(
          bucket: bucket,
          key: "#{prefix}work-mark",
        )
      end

    end

    class HostWork
      def initialize(s3, bucket, prefix, contents, pubdate_str)
        @s3 = s3
        @bucket = bucket
        @prefix = prefix
        @contents = contents
        @pubdate_str = pubdate_str
      end

      attr_reader :bucket, :prefix, :pubdate_str

      def host
        @host ||= prefix.split(?/)[-1]
      end

      def meta
        @meta ||= JSON.parse(
          @s3.get_object(bucket: bucket, key: "#{prefix}meta.json").body.read
        )
      end

      def meta_modified_at
        @meta_modified_at ||= @s3.head_object(bucket: bucket, key: "#{prefix}meta.json").last_modified
      rescue Aws::S3::Errors::NoSuchKey, Aws::S3::Errors::NotFound
        nil
      end

      def error_count
        (meta['try'] || 0) + (meta['single_mp3_path'] ? 0 : 1000)
      end

      def vote_value
        return @vote if @vote_set
        @vote = begin
          @s3.get_object(bucket: bucket, key: "#{prefix}vote.txt").body.read.to_i
        rescue Aws::S3::Errors::NoSuchKey
          -1
        end
        @vote_set = true
        @vote
      end

      def move_to!(new_prefix, skip_single: false, mp3_storage_class: 'REDUCED_REDUNDANCY', storage_class: 'STANDARD')
        keys = @contents.map(&:key)
        keys.each do |key|
          if skip_single
            basename = key.split(?/).last
            next if basename == meta['single_mp3_path']
            next if basename == meta['single_mp4_path']
          end

          sclass = case
                   when key.end_with?('.mp3')
                     mp3_storage_class
                   else
                     storage_class
                   end

          @s3.copy_object(
            storage_class: sclass,
            copy_source: "/#{bucket}/#{URI.encode_www_form_component(key)}",
            bucket: bucket,
            key: "#{new_prefix}#{key[prefix.size..-1]}",
          )
        end
        keys.each do |key|
          @s3.delete_object(
            bucket: bucket,
            key: key,
          )
        end
      end

      def destroy!
        keys = @contents.map(&:key)
        keys.each do |key|
          @s3.delete_object(
            bucket: bucket,
            key: key,
          )
        end
      end

      def extract_works(new_prefix)
        new_meta = meta.dup

        if meta['single_mp3_path']
          @s3.copy_object(
            copy_source: "/#{bucket}/#{URI.encode_www_form_component([prefix,meta['single_mp3_path']].join)}",
            bucket: bucket,
            key: "#{new_prefix}#{pubdate_str}.mp3",
          )
          new_meta['single_mp3_path'] = "/#{new_prefix}#{pubdate_str}.mp3"
        end

        if meta['single_mp4_path']
          @s3.copy_object(
            copy_source: "/#{bucket}/#{URI.encode_www_form_component([prefix,meta['single_mp4_path']].join)}",
            bucket: bucket,
            key: "#{new_prefix}#{pubdate_str}.mp4",
          )
          new_meta['single_mp4_path'] = "/#{new_prefix}#{pubdate_str}.mp4"
        end

        new_meta.delete 'flv_paths'
        new_meta.delete 'mp3_paths'

        @s3.put_object(
          bucket: bucket,
          key: "#{new_prefix}#{pubdate_str}.json",
          body: new_meta.to_json,
          content_type: 'application/json',
        )
      end
    end
  end
end

config_path = "#{__dir__}/config.yml"
if File.exists?(config_path)
  config = YAML.load_file(config_path)
else
  config = {}
end

Aandg::S3Cleaner.new(config).run!
