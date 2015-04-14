# coding: utf-8
require 'nokogiri'
require 'open-uri'

module Aandg
  class Timetable
    STREAMING_TIMETABLE_URL = 'http://www.agqr.jp/timetable/streaming.php'
    AM_TIMETABLE_URL = 'http://www.agqr.jp/timetable/radio.php'

    class ParseError < StandardError; end

    class Program
      def initialize(h={})
        @day = h[:day]
        @starts_at = h[:starts_at]
        @ends_at = h[:ends_at]
        @title = h[:title]
        @personality = h[:personality]
        @link = h[:link]
        @mail = h[:mail]
        @banner = h[:banner]
        @video = h[:video]
        @repeat = h[:repeat]
        @live = h[:live]

        unless @day && @starts_at && @ends_at && @title
          raise ArgumentError, "day, starts_at, ends_at, title is required" 
        end
        if @title.gsub(/\s|　/, '').empty?
          raise ArgumentError, "title is not present"
        end
      end

      attr_reader :day, :starts_at, :ends_at, :title, :personality, :link, :banner

      def duration
        ((ends_at[0] * 60) + ends_at[1]) - ((starts_at[0] * 60) + starts_at[1])
      end

      def repeat?
        @repeat
      end

      def live?
        @live
      end

      def video?
        @video
      end
    end

    def self.streaming
      self.new Nokogiri::HTML(open(STREAMING_TIMETABLE_URL, &:read))
    end

    def self.am
      self.new Nokogiri::HTML(open(AM_TIMETABLE_URL, &:read))
    end

    def initialize(html)
      parse!(html)
    end

    attr_reader :days

    private

    def parse!(html)
      table = html.at('.timetb-am, .timetb-ag')

      # mapping of week of days
      headings = table.at('thead').search('td, th')[1..-1].map(&:inner_text)
      day_map = Hash[headings.map.with_index do |day_ja, i|
        wday = case day_ja.gsub(/\s|　/, '')
        when "月曜日"
          1
        when "火曜日"
          2
        when "水曜日"
          3
        when "木曜日"
          4
        when "金曜日"
          5
        when "土曜日"
          6
        when "日曜日"
          0
        else
          raise ParseError, "Unknown heading at #{i}: #{day_ja.inspect}"
        end
        [i, wday]
      end]

      tds_by_day = {}
      rows = table.at('tbody').search('> tr').map { |tr| tr.search('td') }
      rows.each do |tr|
        tr.each_with_index do |td, i|
          day = day_map[i]
          raise ParseError, "Unknown day #{i}: #{td.inspect}" unless day

          tds_by_day[day] ||= []
          tds_by_day[day] << td
        end
      end

      @days = {}
      tds_by_day.each do |day, tds|
        program_infos = tds.map do |td|
          mail = begin
            mail_link = td.at('.rp a') && td.at('.r-p a')
            if mail_link && mail_link['href'] && mail_link['href'].start_with?('mailto:')
              mail_link['href']
            else
              nil
            end
         end

          classes = (td['class'] || '').split(/\s+/)
          etc = classes.include?('bg-etc')

          time = td.at('.time')
          raise ParseError, ".time not found #{td.inner_text.inspect}" if !time && !etc
          if time
            time_match = time.inner_text.match(/(\d+):(\d+)/)
            raise ParseError, ".time doesn't match HH:MM regexp: #{time.inner_text}" unless time_match
            starts_at = [time_match[1], time_match[2]].map(&:to_i)
          else
            starts_at = nil
          end

          {
            day: day,
            starts_at: starts_at,
            span: td['rowspan'] ? td['rowspan'].to_i : 1,
            title: td.at('.title-p') && td.at('.title-p').inner_text.gsub(/\s|　/, ''),
            personality: td.at('.rp') && td.at('.rp').inner_text.gsub(/\s|　/, ''),
            link: td.at('.title-p a') && td.at('.title-p a')['href'],
            mail: mail,
            banner: td.at('.bnr img') && td.at('.bnr img')['src'],
            video: !!td.at('img[src*="icon_m."]'),
            repeat: !(classes.include?('bg-f') || classes.include?('bg-l')),
            live: classes.include?('bg-l'),
            etc: etc,
          }
        end

        @days[day] = []
        program_infos.each_with_index do |program_info, i|
          next if program_info[:etc]

          next_program = program_infos[i.succ]

          ends_at = if next_program[:starts_at]
            next_program[:starts_at]
          else
            # Assume 1 row = 30 minute
            (
              ((program_info[:starts_at][0] * 60) + program_info[:starts_at][1]) +
              (program_info[:span] * 30)
            ).divmod(60)
          end

          @days[day] << Program.new(program_info.merge(ends_at: ends_at))
        end
      end
    end
  end
end