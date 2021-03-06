#!/usr/bin/env ruby
ENV['TZ'] = 'Asia/Tokyo'
Dir.chdir __dir__
require 'bundler/setup'
require 'pp'
require 'yaml'
require 'time'
require 'uri'
require 'open-uri'
require 'nokogiri'
require 'fluent-logger'
require 'aws-sdk'
require 'json'
require 'fileutils'
require 'socket'
require 'tempfile'
require 'logger'

$stdout.sync = true
@logger = Logger.new($stdout)
@logger.progname = 'rec'
def tweet(message)
end

class Program
  def self.acquire
    self.new open('http://www.uniqueradio.jp/aandg', &:read)
  end

  def initialize(js)
    m = js.match(/^\s*var Program_name = "(.+?)"/)
    @name = URI.decode_www_form_component(m[1]) if m
    m = js.match(/^\s*var Program_img = "(.+?)"/)
    @img = m[1] if m
    m = js.match(/^\s*var Program_link = "(.+?)"/)
    @link = m[1] if m
    m = js.match(/^\s*var Program_text = "(.+?)"/)
    @text = URI.decode_www_form_component(m[1]) if m
    m = js.match(/^\s*var Program_personality = "(.+?)"/)
    @personality = URI.decode_www_form_component(m[1]) if m
    m = js.match(/^\s*var Now_music = "(.+?)"/)
    @music = URI.decode_www_form_component(m[1]) if m && !m[1].empty?
    m = js.match(/^\s*var Now_artist = "(.+?)"/)
    @artist = URI.decode_www_form_component(m[1]) if m && !m[1].empty?
  end

  attr_reader :name, :img, :link, :text, :personality, :music, :artist

  def inspect
    "#<Program #{@name} / #{@personality}>"
  end
end

class FmsList
  class Server < Struct.new(:cryptography, :protocol, :server, :app, :stream)
    def encrypted?
      !cryptography.empty?
    end

    def rtmp
      "#{protocol}://#{server.sub(/\/.*$/,'/')}"
    end

    alias app_orig app

    def app
      "?rtmp://#{server.sub(/^.*\//,'')}/#{app_orig}/"
    end

    def playpath
      stream
    end
  end

  def self.acquire
    self.new Nokogiri::XML(open('http://www.uniqueradio.jp/agplayerf/getfmsListHD.php'))
  end

  def initialize(xml)
    @servers = xml.search('ag serverlist serverinfo').map do |serverinfo|
      Server.new(*%w(cryptography protocol server app stream).map { |_|
        serverinfo.at(_).text
      })
    end
  end

  def available_servers
    @servers.reject { |server| server.encrypted? }
  end

  attr_reader :servers
end

if ARGV.size < 2
  abort "usage: #{$0} name seconds [start]"
end

name, seconds, start = *ARGV
seconds = seconds.to_i

config_path = "#{__dir__}/config.yml"
if File.exists?(config_path)
  config = YAML.load_file(config_path)
else
  config = {}
end

RECORD_DIR = ENV['AGQR_RECORD_DIR'] || config['record_dir'] || "#{__dir__}/recorded"
LOG_DIR = Pathname.new(ENV['AGQR_LOG_DIR'] || config['log'] || './log').tap(&:mkpath)
S3_REGION = ENV['AGQR_S3_REGION'] || config['s3_region']
S3_BUCKET = ENV['AGQR_S3_BUCKET'] || config['s3_bucket']
S3_PREFIX = (ENV['AGQR_S3_PREFIX'] || config['s3_prefix'] || '').sub(/\/\z/,'')
S3_ACCESS_KEY_ID = config['aws_access_key_id']
S3_SECRET_ACCESS_KEY =  config['aws_secret_access_key']
HTTP_BASE = ENV['AGQR_URL_BASE'] || config['http_base'] || "http://localhost"
MARGIN_BEFORE = (ENV['AGQR_MARGIN_BEFORE'] || config['margin_before'] || 12).to_i
MARGIN_AFTER = (ENV['AGQR_MARGIN_AFTER'] || config['margin_after'] || 20).to_i
ALLOW_EARLY_EXIT = (ENV['AGQR_EARLY_EXIT_ALLOWANCE'] || config['allow_early_exit'] || 10).to_i
HOSTNAME = (ENV['AGQR_HOSTNAME'] || Socket.gethostname)
TIMEOUT = (ENV['AGQR_TIMEOUT'] || config['timeout'] || 10).to_i

raise 'specify s3_bucket and s3_region' unless S3_BUCKET && S3_REGION

if start
  if start.size == 4
    h,m = start[0,2].to_i, start[2,2].to_i
    now = Time.now
    time = Time.new(now.year, now.month, now.day, h, m, 0)
    time += 86400 if time < now
  else
    time = Time.at(start.to_i)
  end

  waittime = time - MARGIN_BEFORE
  @logger.info "Sleep until #{waittime} "
  sleep 1 until waittime <= Time.now
end

pubdate = time || Time.now
pubdate_str = pubdate.strftime('%Y-%m-%d_%H%M%S')

safe_name = name.
  gsub("/", "／").
  tr("[](){}", "［］（）｛｝").
  gsub('"','').
  gsub("'", '').
  gsub("　", " ").
  gsub(" ", "_")

target_dir = File.join(RECORD_DIR, "#{pubdate_str}.#{$$}")
FileUtils.mkdir_p(target_dir) unless File.exists?(target_dir)

prog = nil
Thread.new { # acquire program information after few seconds
  if seconds < 10
    sleep 0
  elsif seconds < 70
    sleep 30
  else
    sleep 60
  end

  prog = Program.acquire
  @logger.info "=> program: #{prog.name}"
  @logger.info "   #{prog.text.inspect}"
  tweet "agqr.#{name}.watching: #{prog.name} (#{pubdate})"
}

servers = FmsList.acquire.available_servers

try = 0
if start
  stop = MARGIN_BEFORE+seconds+MARGIN_AFTER
else
  stop = seconds
end
flv_paths = []

2.times do
  servers.each do |server|
    3.times do |server_try|
      break if stop < 1
      flv_path = File.join(target_dir, "#{try}.flv")
      flv_paths << flv_path
      cmd = [
        'rtmpdump',
#        '--verbose',
        '--live',
        '-o', flv_path,
        '--stop', stop.to_i,
        '--timeout', TIMEOUT,
        '--rtmp', server.rtmp,
        '--app', server.app,
        '--playpath', server.playpath,
      ].map(&:to_s)
      record_start = Time.now
      @logger.info "==> #{cmd.join(' ')}"
      tweet "agqr.#{name}.start: #{stop} seconds (try:#{try}, #{pubdate})"

      status = nil
      
      log_path = LOG_DIR.join("rtmpdump.#{$$}.#{try}.log")
      open(log_path, 'w') do  |log_io|
        log_io.puts "=> #{safe_name}: #{cmd.inspect}"
        log_io.flush
        pid = spawn(*cmd, out: log_io, err: log_io)
        pid, status = Process.waitpid(pid)
      end

      elapsed = (Time.now - record_start).to_i
      if status && !status.success?
        @logger.warn "May be failed"
        tweet "agqr.#{name}.fail: #{pubdate.rfc2822}"
      elsif /^Download may be incomplete/ === File.read(log_path)
        @logger.warn "Download may be incomplete"
        tweet "agqr.#{name}.incomplete: #{pubdate.rfc2822}"
      elsif elapsed < (seconds-ALLOW_EARLY_EXIT)
        @logger.warn "Exited earlier (#{elapsed} seconds elapsed, #{stop} seconds expected)"
        tweet "agqr.#{name}.early-exit: #{pubdate.rfc2822}; #{elapsed}s elapsed, #{stop}s expected"
      else
        @logger.info "ok"
        if prog
          tweet "agqr.#{name}.watched: #{prog.name} (#{pubdate.to_i})"
        else
          tweet "agqr.#{name}.watched: #{pubdate.rfc2822}"
        end

        break nil
      end

      try += 1
      stop -= elapsed
      sleep 2
    end || break
  end || break
end

mp3_paths = nil
single_mp3_path =nil
single_mp4_path = nil
open(LOG_DIR.join("ffmpeg.#{$$}.log"), 'w') do  |log_io|
  mp3_paths = flv_paths.map do |flv_path|
    mp3_path = flv_path.sub(/\.flv$/, '.mp3')

    cmd = ["ffmpeg", "-i", flv_path, "-b:a", "64k", mp3_path]
    @logger.info "==> #{cmd.join(' ')}"
    log_io.puts "=> #{safe_name}: #{cmd.inspect}"
    log_io.flush

    status = system(*cmd, out: log_io, err: log_io)
    if status
      @logger.info "ok"
      mp3_path
    else
      @logger.error "mp3 encoding Failed ;("
      nil
    end
  end.compact

  @logger.info "==> Concatenating MP3"
  single_mp3_path = File.join(target_dir, 'all.mp3')
  playlist = Tempfile.new("agqr-#{pubdate_str}-#{$$}-mp3.txt")
  playlist.puts mp3_paths.map { |_| "file '#{_}'" }.join("\n")
  playlist.flush
  cmd = ["ffmpeg", "-f", "concat", "-i", playlist.path, "-c", "copy", single_mp3_path]
  @logger.info "#{cmd.join(' ')}"
  log_io.puts "=> #{safe_name}: #{cmd.inspect}"
  log_io.flush

  status = system(*cmd, out: log_io, err: log_io)
  if status
    @logger.info "ok"
  else
    @logger.error "mp3 concat Failed ;("
    nil
  end

  @logger.info "==> Concatenating FLV"
  single_mp4_path = File.join(target_dir, 'all.mp4')
  playlist = Tempfile.new("agqr-#{pubdate_str}-#{$$}-mp4.txt")
  playlist.puts flv_paths.map { |_| "file '#{_}'" }.join("\n")
  playlist.flush
  cmd = ["ffmpeg", "-f", "concat", "-i", playlist.path, "-vcodec", "libx264", "-acodec", "libfaac", "-b:a", "64k", single_mp4_path]
  @logger.info "#{cmd.join(' ')}"
  log_io.puts "=> #{safe_name}: #{cmd.inspect}"
  log_io.flush

  status = system(*cmd, out: log_io, err: log_io)
  if status
    @logger.info "ok"
  else
    @logger.error "FLV->MP4 Failed ;("
    nil
  end
end

@logger.info "==> Generating metadata"
meta_path = File.join(target_dir, 'meta.json')
meta = {
  host: HOSTNAME,
  try: try,
  date: {
    unix: pubdate.to_i,
    str: pubdate_str,
    pubdate: pubdate.rfc2822,
  },
  flv_paths: flv_paths.map { |_| File.basename(_) },
  mp3_paths: mp3_paths.map { |_| File.basename(_) },
}
if File.exist?(single_mp3_path)
  meta[:single_mp3_path] = File.basename(single_mp3_path)
end
if File.exist?(single_mp4_path)
  meta[:single_mp4_path] = File.basename(single_mp4_path)
end
if prog
  meta.merge!(
    program: {
      title: prog.name,
      description: prog.text,
      link: prog.link,
      personality: prog.personality,
    }
  )
end
@logger.info meta.inspect
File.write meta_path, "#{meta.to_json}\n"

@logger.info "==> Uploading to S3"
if S3_BUCKET && S3_REGION
  if S3_ACCESS_KEY_ID && S3_SECRET_ACCESS_KEY
    s3 = Aws::S3::Client.new(region: S3_REGION, credentials: Aws::Credentials.new(S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY), logger: @logger)
  else
    s3 = Aws::S3::Client.new(region: S3_REGION, logger: @logger)
  end

  s3_key_base = "#{S3_PREFIX}/#{safe_name}/work/#{pubdate_str}/#{HOSTNAME}"

  flv_paths.each do |_|
    open(_, 'r') do |io|
      key = "#{s3_key_base}/#{File.basename(_)}"
      @logger.info "#{_} => s3://#{S3_BUCKET}/#{key} @ #{S3_REGION}"
      s3.put_object(
        bucket: S3_BUCKET,
        key: key,
        body: io,
        content_type: 'video/x-flv',
      )
    end
  end

  mp3_paths.each do |_|
    open(_, 'r') do |io|
      key = "#{s3_key_base}/#{File.basename(_)}"
      @logger.info "#{_} => s3://#{S3_BUCKET}/#{key} @ #{S3_REGION}"
      s3.put_object(
        bucket: S3_BUCKET,
        key: key,
        body: io,
        content_type: 'audio/mpeg',
      )
    end
  end

  if File.exist?(single_mp3_path)
    open(single_mp3_path, 'r') do |io|
      key = "#{s3_key_base}/#{File.basename(single_mp3_path)}"
      @logger.info "#{single_mp3_path} => s3://#{S3_BUCKET}/#{key} @ #{S3_REGION}"
      s3.put_object(
        bucket: S3_BUCKET,
        key: key,
        body: io,
        content_type: 'audio/mpeg',
      )
    end
  end

  if File.exist?(single_mp4_path)
    open(single_mp4_path, 'r') do |io|
      key = "#{s3_key_base}/#{File.basename(single_mp4_path)}"
      @logger.info "#{single_mp4_path} => s3://#{S3_BUCKET}/#{key} @ #{S3_REGION}"
      s3.put_object(
        bucket: S3_BUCKET,
        key: key,
        body: io,
        content_type: 'video/mpeg',
      )
    end
  end

  open(meta_path, 'r') do |io|
    key = "#{s3_key_base}/#{File.basename(meta_path)}"
    @logger.info "#{meta_path} => s3://#{S3_BUCKET}/#{key} @ #{S3_REGION}"
    s3.put_object(
      bucket: S3_BUCKET,
      key: key,
      body: io,
      content_type: 'application/json',
    )
  end

  vote = rand(1000)
  @logger.info " * Vote #{vote}"
  key = "#{s3_key_base}/vote.txt"
  s3.put_object(
    bucket: S3_BUCKET,
    key: key,
    body: vote.to_s,
    content_type: 'text/plain',
  )
else
  @logger.info "Skipping"
end

FileUtils.remove_entry_secure(target_dir)

if prog
  tweet "agqr.#{name}.done: #{prog.name} (#{pubdate.to_i})"
else
  tweet "agqr.#{name}.done: #{pubdate.rfc2822}"
end

@logger.info "Done!"
