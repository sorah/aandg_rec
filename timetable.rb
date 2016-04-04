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

      def to_h
        {
          day: @day,
          starts_at: @starts_at,
          ends_at: @ends_at,
          title: @title,
          personality: @personality,
          link: @link,
          mail: @mail,
          banner: @banner,
          video: @video,
          repeat: @repeat,
          live: @live,
        }
      end

      def duration
        ((ends_at[0] * 60) + ends_at[1]) - ((starts_at[0] * 60) + starts_at[1])
      end

      def duration_sec
        duration * 60
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

      def starts_at_sec
        ((starts_at[0] * 60) + starts_at[1]) * 60
      end

      def next_occasion_from(time)
        time_sec = (((time.hour * 60) + time.min) * 60) + time.sec

        if day == time.wday && time_sec <= starts_at_sec
          Time.new(time.year,time.month,time.day,starts_at[0],starts_at[1],0)
        else
          remaining_days = day <= time.wday ?  ((7 - time.wday) + day) : (day - time.wday)
          wdayadj = (time + ( remaining_days * 86400 ))
          Time.new(wdayadj.year,wdayadj.month,wdayadj.day,starts_at[0],starts_at[1],0)
        end
      end

      def inspect
        "#<#{self.class.name}: #{title.inspect} / day #{day}, #{starts_at.map { |_| _.to_s.rjust(2,?0) }.join(?:)}-#{ends_at.map { |_| _.to_s.rjust(2,?0) }.join(?:)} (#{duration}m)>"
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

    def ==(o)
      self.class == o.class && self.for_compare == o.for_compare
    end

    def for_compare
      @days.map do |day, progs|
        [day, progs.map(&:to_h)]
      end
    end

    def take(n, starting: Time.now)
      starting_sec = (((starting.hour * 60) + starting.min) * 60) + starting.sec
      result = []

      time = starting
      wday = time.wday
      until result.size >= n
        programs = days[wday] or raise "no programs for wday #{wday}"
        if result.empty?
          programs = programs.reject do |prog|
            prog.starts_at_sec < starting_sec
          end
        end

        programs.each do |prog|
          time = prog.next_occasion_from(time)
          result << [time, prog]
          break if result.size >= n
        end
        wday = wday.succ % 7
      end
      result
    end

    private

    def parse!(html)
      table = html.at('.timetb-am, .timetb-ag')

      # mapping of week of days
      headings = table.at('thead').search('td, th').map(&:inner_text)
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
        when ""
          -1
        else
          raise ParseError, "Unknown heading at #{i}: #{day_ja.inspect}"
        end
        [i, wday]
      end]

      rows = table.at('tbody').search('> tr').map { |tr| tr.search('td') }
      rows_count = rows.map(&:size).max
      # fill nil for unexist cell, where previous <td> is continuing due to rowspan>1
      rowspan_state = {}
      padded_rows = rows.map do |row|
        expected_col_count = rowspan_state.size + row.size
        padded_row = []

        rowp = 0
        while padded_row.size < expected_col_count
          if rowspan_state[padded_row.size]
            padded_row << nil
          else
            rowspan = (row[rowp]['rowspan'] || 1).to_i
            if rowspan > 1
              rowspan_state[padded_row.size] = rowspan
            end

            padded_row << row[rowp]
            rowp += 1
          end
        end

        rowspan_state.each_key do |colnum|
          rowspan_state[colnum] -= 1
          rowspan_state.delete(colnum) if rowspan_state[colnum] < 1
        end

        padded_row
      end


      tds_by_day = {}
      padded_rows.each do |tr|
        tr.each_with_index do |td, i|
          next unless td
          day = day_map[i]
          next if day < 0
          raise ParseError, "Unknown day #{i}: #{td.inspect}" unless day

          tds_by_day[day] ||= []
          tds_by_day[day] << td
        end
      end

      @days = {}
      tds_by_day.each do |day, tds|
        prev_hour = nil
        midnight = false

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

          if prev_hour && starts_at && prev_hour > starts_at[0]
            midnight = true
          end
          prev_hour = starts_at[0] if starts_at
          if midnight
            program_day = day.succ % 7
          else
            program_day = day
          end

          {
            day: program_day,
            starts_at: starts_at,
            span: td['rowspan'] ? td['rowspan'].to_i : 1,
            title: td.at('.title-p') && td.at('.title-p').inner_text.sub(/^(\s|　)+/, '').sub(/(\s|　)+$/, ''),
            personality: td.at('.rp') && td.at('.rp').inner_text.sub(/^(\s|　)+/, '').sub(/(\s|　)+$/, ''),
            link: td.at('.title-p a') && td.at('.title-p a')['href'],
            mail: mail,
            banner: td.at('.bnr img') && td.at('.bnr img')['src'],
            video: !!td.at('img[src*="icon_m."]'),
            repeat: !(classes.include?('bg-f') || classes.include?('bg-l')),
            live: classes.include?('bg-l'),
            etc: etc,
          }
        end

        program_infos.group_by { |_| _[:day] }.each do |program_day, pis|
          @days[program_day] ||= []
          pis.each_with_index do |program_info, i|
            next if program_info[:etc]

            next_program = pis[i.succ] || program_infos.find{ |_| _[:day] == (program_day.succ%7) }

            ends_at = if next_program[:starts_at]
              next_start = next_program[:starts_at].dup
              if next_start[0] < program_info[:starts_at][0]
                next_start[0] += 24
              end
              next_start
            else
              # Assume 1 row = 30 minute
              (
                ((program_info[:starts_at][0] * 60) + program_info[:starts_at][1]) +
                (program_info[:span] * 30)
              ).divmod(60)
            end

            @days[program_day] << Program.new(program_info.merge(ends_at: ends_at))
          end
          @days[program_day].sort_by! { |_| _.starts_at }
        end
      end
    end
  end

  class DummyTimetable < Timetable
    def initialize
      @days = {}
      0.upto(6) do |day|
        @days[day] = 0.step((60*24)-2,2).map do |start|
          Program.new(day: day, starts_at: start.divmod(60), ends_at: (start+2).divmod(60), title: "dummy-#{start}")
        end
      end
    end
  end
end
