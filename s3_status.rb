#!/usr/bin/env ruby
ENV['TZ'] = 'Asia/Tokyo'
require 'bundler/setup'
require_relative './s3_cleanup'

config_path = "#{__dir__}/config.yml"
if File.exists?(config_path)
  config = YAML.load_file(config_path)
else
  config = {}
end

cleaner = Aandg::S3Cleaner.new(config, logger: Logger.new(File::NULL))

case ARGV[0]
when 'pending_count'
  puts cleaner.existing_work_prefixes.size
when 'invalid_pending_count'
  puts cleaner.existing_work_prefixe.select { |_| _.vote_winner.nil? || _.best_work.nil? }.size
when 'help'
  puts "#{$0}"
  puts "#{$0} pending_count"
  puts "#{$0} invalid_pending_count"
  puts "#{$0} help"
  exit 1
else
  cleaner.program_prefixes.each do |prog|
    puts "=> #{prog.prefix} (recorded #{prog.recordings.size} times)"
    prog.work_prefixes.each do |work|
      winner = work.vote_winner
      winner_str = if winner
                     "#{winner.host} leads"
                   else
                     "NO LEADER"
                   end

      best_work = work.best_work
      best_work_str = if best_work
                        "#{best_work.host} has the best"
                      else
                        "NO BEST WORK"
                      end

      puts " * #{work.prefix} (#{winner_str}, #{best_work_str})"
      if !work.target
        puts "   (not target yet)"
      end

      work.host_works.each do |host|
        puts "    - #{host.prefix} (#{host.error_count} err)"
      end
    end
  end
end
