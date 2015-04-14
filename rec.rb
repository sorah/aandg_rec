#!/usr/bin/env ruby
Dir.chdir __dir__
require 'pp'
require 'yaml'
require 'time'
require 'bundler/setup'
require 'uri'
require 'open-uri'
require 'nokogiri'
require 'fluent-logger'

@logger = Fluent::Logger::FluentLogger.new("recorder", :host=>'127.0.0.1', :port=>24224)
def tweet(message)
  @logger.post("aandg", message: message)
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
RECORD_DIR = config['record_dir'] || "#{__dir__}/recorded"
HTTP_BASE = config['http_base'] || "http://localhost"
MARGIN_BEFORE = (config['margin_before'] || 12).to_i
MARGIN_AFTER = (config['margin_after'] || 20).to_i
ALLOW_EARLY_EXIT = (config['allow_early_exit'] || 10).to_i
TIMEOUT = (config['timeout'] || 10).to_i

target_dir = File.join(RECORD_DIR,name)
Dir.mkdir(target_dir) unless File.exists?(target_dir)

rss_path = File.join(target_dir, 'index.xml')

if start
  h,m = start[0,2].to_i, start[2,2].to_i if start
  now = Time.now
  time = Time.new(now.year, now.month, now.day, h, m, 0)
  time += 86400 if time < now

  waittime = time - MARGIN_BEFORE
  puts "  * Sleep until #{waittime} "
  sleep 1 until waittime <= Time.now
end

pubdate = time || Time.now
flv_path_base = File.join(target_dir, pubdate.strftime('%Y-%m-%d_%H%M%S.try.flv'))

prog = nil
Thread.new { # acquire program information after few seconds
  if seconds < 10
    sleep 5
  elsif seconds < 70
    sleep 30
  else
    sleep 60
  end

  prog = Program.acquire
  puts "  * #{prog.name}"
  puts "  * #{prog.text.inspect}"
  tweet "aandg.#{name}.watching: #{prog.name} (#{pubdate.to_i})"
}

servers = FmsList.acquire.available_servers

try = 0
stop = MARGIN_BEFORE+seconds+MARGIN_AFTER
flv_paths = []
servers.each do |server|
  2.times do
    break if stop < 1
    flv_path = flv_path_base.sub(/\.try\./,".#{try}.")
    flv_paths << flv_path
    cmd = [
      'rtmpdump',
      '--verbose',
      '--live',
      '-o', flv_path,
      '--stop', stop,
      '--timeout', TIMEOUT,
      '--rtmp', server.rtmp,
      '--app', server.app,
      '--playpath', server.playpath,
    ].map(&:to_s)
    record_start = Time.now
    puts "==> #{cmd.join(' ')}"
    tweet "aandg.#{name}.start: #{stop} seconds (try:#{try}, #{pubdate})"

    status = nil
    out = ""
    IO.popen([*cmd, err: [:child, :out]], 'r') do |io|
      th = Thread.new {
        begin
          buf = ""
          until io.eof?
            str =  io.read(10)
            buf << str; out << str
            lines = buf.split(/\r|\n/)
            if 1 < lines.size
              buf = lines.pop
              lines.each do |line|
                puts line
              end
            end
          end
        rescue Exception => e
          p e
          puts e.backtrace
        end
      }

      pid, status = Process.waitpid(io.pid)

      th.kill if th && th.alive?
    end

    elapsed = Time.now - record_start
    if status && !status.success?
      puts "  * May be fail"
      tweet "aandg.#{name}.fail: #{pubdate.rfc2822}"
    elsif /^Download may be incomplete/ === out
      puts "  * Download may be incomplete"
      tweet "aandg.#{name}.incomplete: #{pubdate.rfc2822}"
    elsif elapsed < seconds-ALLOW_EARLY_EXIT
      puts "  * Exited earlier (#{elapsed} seconds elapsed, #{stop} seconds expected)"
      tweet "aandg.#{name}.early-exit: #{pubdate.rfc2822}; #{elapsed} seconds elapsed / #{stop} seconds expected"
    else
      puts "  * Done!"
      if prog
        tweet "aandg.#{name}.watched: #{prog.name} (#{pubdate.to_i})"
      else
        tweet "aandg.#{name}.watched: #{pubdate.rfc2822}"
      end

      break
    end

    try += 1
    stop -= elapsed
  end || break
end

mp3_paths = flv_paths.map do |flv_path|
  mp3_path = flv_path.sub(/\.flv$/, '.mp3')

  cmd = ["ffmpeg", "-i", flv_path, "-b:a", "64k", mp3_path]
  puts "==> #{cmd.join(' ')}"

  status = system(*cmd)
  if status
    puts "  * Done!"
    mp3_path
  else
    puts "  * Failed ;("
    nil
  end
end.compact

puts "==> Generating RSS"

oldxml = Nokogiri::XML(File.read(rss_path)) if File.exists?(rss_path)
builder = Nokogiri::XML::Builder.new do |xml|
  xml.rss('xmlns:itunes' => "http://www.itunes.com/dtds/podcast-1.0.dtd", version: '2.0') {
    xml.channel {
      if prog
        xml.title prog.name.gsub(/<.+?>/, ' ')
        xml.description prog.text.gsub(/<.+?>/,'')
        xml.link prog.link
        xml['itunes'].author prog.personality
      elsif oldxml
        xml.title oldxml.at('rss channel title').text
        xml.description oldxml.at('rss channel description').text
        xml.link oldxml.at('rss channel link').text
        xml['itunes'].author oldxml.at('rss channel itunes|author').text
      else
        xml.title name
        xml.description '-'
        xml.link 'http://localhost/'
        xml['itunes'].author '-'
      end

      xml.lastBuildDate Time.now.rfc2822
      xml.language 'ja'

      mp3_paths.reverse.each_with_index do |mp3_path,i|
        next unless File.exists?(mp3_path)
        xml.item {
          xml.title "#{pubdate.strftime("%Y/%m/%d %H:%M")}#{0 < i ? "-#{i+1}" : ""} #{prog.name} - #{prog.personality}"
          xml.description prog.text.gsub(/<.+?>/,'')
          link = "#{HTTP_BASE}/#{name}/#{File.basename(mp3_path)}"
          xml.link link
          xml.guid link
          xml.author prog.personality
          xml.pubDate pubdate.rfc2822
          xml.enclosure(url: link, length: File.stat(mp3_path).size, type: 'audio/mpeg')
        }
      end

      if oldxml
        oldxml.search('rss channel item').each do |olditem|
          xml.item {
            xml.title olditem.at('title').text
            xml.description olditem.at('description').text
            xml.link olditem.at('link').text
            xml.guid olditem.at('guid').text
            xml.author olditem.at('author').text
            xml.pubDate olditem.at('pubDate').text
            xml.enclosure(
              url: olditem.at('enclosure')['url'],
              length: olditem.at('enclosure')['length'],
              type: olditem.at('enclosure')['type'],
            )
          }
        end
      end
    }
  }
end

File.write rss_path, builder.to_xml
if prog
  tweet "aandg.#{name}.done: #{prog.name} (#{pubdate.to_i})"
else
  tweet "aandg.#{name}.done: #{pubdate.rfc2822}"
end


