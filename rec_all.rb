#!/usr/bin/env ruby
ENV['TZ'] = 'Asia/Tokyo'
Dir.chdir __dir__
require 'bundler/setup'
require_relative './timetable'
require 'yaml'
require 'pathname'
require 'thread'
require 'sigdump/setup'
require 'sleepy_penguin'


module Aandg
  class Scheduler
    def initialize(config)
      @shutdown = nil
      @log_dir = Pathname.new(ENV['AGQR_LOG_DIR'] || config['log'] || './log').tap(&:mkpath)
    end

    def start
      raise 'already ran' unless @shutdown.nil?
      @shutdown = false

      @cleanup_invoker = CleanupInvoker.new(log_dir: @log_dir)
      @cleanup_invoker.start
      @cleanup_invoker.request!

      @timetable_updater = TimetableUpdater.new.tap do |tt|
        tt.on_update(&method(:on_timetable_update))
        tt.start
      end

      @recorder_invoker = RecorderInvoker.new(log_dir: @log_dir).tap do |rec|
        rec.on_start(&method(:on_record_start))
        rec.on_complete(&method(:on_record_complete))
        rec.start
      end
    end

    def run!
      start

      stop_event = SleepyPenguin::EventFD.new(0, :SEMAPHORE)
      term_event = SleepyPenguin::EventFD.new(0, :SEMAPHORE)

      Thread.start { stop_event.value; stop }
      Thread.start { term_event.value; terminate }

      int_received = 0
      handler = proc do
        int_received += 1
        case
        when int_received == 1
          stop_event.incr(1)
        when int_received == 2
          term_event.incr(1)
        when int_received == 3
          exit 1
        end
      end
      trap(:INT, handler)
      trap(:TERM, handler)

      @timetable_updater.join
      @cleanup_invoker.join
      @recorder_invoker.join
    end

    def on_timetable_update(timetable)
      if ENV['AGQR_DUMMY_TT'] == '1'
        timetable = Aandg::DummyTimetable.new
      end
      @recorder_invoker.timetable = timetable
    end

    def on_record_start(program)
      $0 = "agqr-rec-all: (#{Time.now.to_i}) #{program.title.inspect}"
    end

    def on_record_complete(program)
      @cleanup_invoker.request!
    end

    def terminate
      terminate!
      wait_down
    end

    def stop
      stop!
      wait_down
    end

    def terminate!
      shutdown!(:immediately)
    end

    def stop!
      shutdown!
    end

    def shutdown!(immediately = false)
      return if @shutdown.nil?
      return if @shutdown
      puts "[scheduler] requesting shutdown#{immediately ? ' (immediately)' : nil}"
      @shutdown = true
      @timetable_updater.shutdown(immediately)
      @cleanup_invoker.shutdown(immediately)
      @recorder_invoker.shutdown(immediately)
    end

    def wait_down
      return if @shutdown.nil?
      puts "[scheduler] waiting shutdown..."
      while @timetable_updater.running? || @cleanup_invoker.running? || @recorder_invoker.running?
        # puts "[scheduler] timetable_updater:#{@timetable_updater.running?}" \
        #      " cleanup_invoker:#{@cleanup_invoker.running?}" \
        #      " recorder_invoker:#{@recorder_invoker.running?}"
        sleep 1
      end
    end

    class Worker
      def running?
        @thread && @thread.alive?
      end

      def shutdown(immediately = false)
        return unless running?
        @stop_event.incr(1)
        if immediately
          terminate
        else
          teardown
        end
      end

      def start
        return if running?
        @stop_event = SleepyPenguin::EventFD.new(0, :SEMAPHORE)
        setup
        setup_poll
        @thread = Thread.new(&method(:main_loop))
      end

      def join
        return unless running?
        @thread.join
      end

      private

      def setup_poll
        @poll = SleepyPenguin::Epoll.new
        @poll.add(@stop_event, [:IN])
      end

      def main_loop
      end

      def setup
      end

      def teardown
        terminate
      end

      def terminate
      end
    end

    class RecorderInvoker < Worker
      def initialize(log_dir: nil)
        @on_start = proc { |prog|  }
        @on_complete = proc { |prog|  }
        @timerfds = {}
        @pids_lock = Mutex.new
        @pids = {}
        @timetable = nil
        @log_dir = log_dir
        @shutdown = nil
      end

      def on_start(&block)
        @on_start = block
      end

      def on_complete(&block)
        @on_complete = block
      end

      def timetable=(tt)
        @timetable = tt
        @reload_event.incr(1) if @reload_event
        tt
      end

      def running?
        super && (@waiter_thread && @waiter_thread.alive?)
      end

      def join
        super
        @waiter_thread.join
      end

      def teardown
        puts "[recorder_invoker] teardown..."
        @pids_lock.synchronize do
          puts "[recorder_invoker] #{@pids.size} recorders are running:"
          @pids.each do |pid, prog|
            puts "[recorder_invoker]   * #{pid}: #{prog.inspect}"
          end
        end
        @shutdown = true
        @waiter_queue << nil
      end

      def terminate
        teardown
        puts "[recorder_invoker] terminating"
        @pids_lock.synchronize do
          @pids.each do |pid, prog|
            begin
              puts "[recorder_invoker] SIGTERM => #{pid} (#{prog.inspect})"
              Process.kill(:TERM, pid)
            rescue Errno::ESRCH, Errno::ECHILD
            end
          end
        end
        @waiter_thread.join(5)
        @waiter_thread.kill if @waiter_thread.alive?
      end

      private

      def setup
        @shutdown = false
        @waiter_queue = Queue.new
        @waiter_thread = Thread.new(&method(:waiter_thread))

        @reload_event = SleepyPenguin::EventFD.new(0, :SEMAPHORE)

        update_timers!
      end

      def setup_poll
        super
        @poll.add(@reload_event, [:IN])
        @timerfds.each_value do |timer, start, prog|
          @poll.add(timer, [:IN])
        end
      end

      def waiter_thread
        while th = @waiter_queue.pop
          th.join
          puts "[recorder_invoker/waiter_thread] remaining #{@waiter_queue.size-1} threads" if @shutdown
        end
      end

      def update_timers!
        return unless @timetable
        puts "[recorder_invoker] Updating timers..."
        @timerfds = {}
        programs = @timetable.take(20)
        programs.each do |start, prog|
          timer = SleepyPenguin::TimerFD.new(:REALTIME)
          timer.settime(:ABSTIME, 0, start.to_i - 60)
          @timerfds[timer.fileno] = [timer, start, prog]
          puts "[recorder_invoker] fd=#{timer.fileno} starts #{start.inspect}: #{prog.inspect}"
        end
      end

      def main_loop
        puts "[recorder_invoker] helo"
        catch(:stop) do
          loop do
            catch(:reload) do
              loop do
                wait_poll
              end
            end
            puts "[recorder_invoker] reload"
            update_timers!
            setup_poll
          end
        end
        puts "[recorder_invoker] exiting"
      rescue Exception => e
        $stderr.puts "[recorder_invoker] !!! encountered error: #{$!.inspect}"
        $!.backtrace.each {|bt| $stderr.puts "[recorder_invoker]   #{bt}" }
        raise
      ensure
        puts "[recorder_invoker] bye"
        @poll.close
      end

      def wait_poll
        @poll.wait do |events, io|
          case io
          when @stop_event
            io.value
            throw :stop
          when @reload_event
            io.value
            throw :reload
          when SleepyPenguin::TimerFD
            io.expirations
            @poll.del(io)
            job = @timerfds.delete(io.fileno)
            if job
              timer, start, prog = job
              invoke_recorder(start, prog)
            else
              $stderr.puts "[recorder_invoker] unknown timer: #{io.fileno}"
            end
            puts "[recorder_invoker] remaining timers: #{@timerfds.size}"
            if @timerfds.size < 2
              throw :reload
            end
          end
        end
      end

      def invoke_recorder(start, prog)
        puts "[recorder_invoker] spawning rec2.rb for #{prog.inspect}"
        if @log_dir
          log_io = open(@log_dir.join("recorder.#{start.to_i}.log"), 'w')
          log_io.puts "=> #{start.inspect} #{prog.inspect}"
          log_io.flush
          opts = {out: log_io, err: log_io}
        else
          opts = {}
        end
        title = prog.repeat? ? "#{prog.title}-repeat" : prog.title
        pid = spawn("#{__dir__}/rec2.rb", title, prog.duration_sec.to_s, start.to_i.to_s, opts)
        log_io.close if log_io
        puts "[recorder_invoker] spawned pid=#{pid} for #{prog.inspect} (#{start.to_i})"
        @pids_lock.synchronize do
          @pids[pid] = prog
        end
        @waiter_queue << Thread.new(prog, pid, &method(:watchdog))
        pid
      end

      def watchdog(prog, pid)
        begin
          @on_start.call(prog) if @on_start
        rescue Exception => e
          $stderr.puts "[recorder_invoker/watchdog(#{pid})/on_start] !!! encountered error: #{$!.inspect}"
          $!.backtrace.each {|bt| $stderr.puts "[recorder_invoker/watchdog(#{pid})/on_start]   #{bt}" }
        end

        puts "[recorder_invoker/watchdog(#{pid})] grrrr..."
        _, status = Process.waitpid2(pid)
        io = status.success? ? $stdout : $stderr
        io.puts "[recorder_invoker/watchdog(#{pid})] #{prog.inspect} finished: #{status.inspect}"
        @pids_lock.synchronize do
          @pids.delete pid
        end
        @on_complete.call(prog) if @on_complete
      rescue Errno::ESRCH, Errno::ECHILD => e
        $stderr.puts "[recorder_invoker/watchdog(#{pid})] abort #{e.inspect}"
      rescue Errno::EINTR
        $stderr.puts "[recorder_invoker/watchdog(#{pid})] EINT"
        sleep 1
        retry
      end
    end

    class CleanupInvoker < Worker
      MARGIN = ENV['AGQR_DEBUG'] == '1' ? 5 : 530

      def initialize(log_dir: nil)
        @lock = Mutex.new
        @log_dir = log_dir
        @pid = nil
      end

      def running?
        super && watchdog_running?
      end

      def watchdog_running?
        @watchdog && @watchdog.alive?
      rescue NoMethodError
        false
      end

      def join
        super
        watchdog = @lock.synchronize { @watchdog }
        watchdog.join if watchdog
      end

      def request!
        remain = @timer.gettime.last
        if remain > 0
          puts "[cleanup_invoker] Already requested, it'll run after #{remain} seconds"
          nil
        else
          val = ENV['AGQR_DEBUG'] == '1' ? MARGIN : MARGIN + rand(60)
          puts "[cleanup_invoker] Requesting with interval of #{val} seconds"
          @timer.settime(0, val, val)
          val
        end
      end

      def perform!
        if @pid
          $stderr.puts "[cleanup_invoker] previous one is still running! (pid=#{@pid})"
          return
        end
        puts "[cleanup_invoker] performing clean..."

        if @log_dir
          log_io = open(@log_dir.join("cleaner.log"), 'a')
          log_io.puts "=> run at #{Time.now.to_s}"
          log_io.flush
          opts = {out: log_io, err: log_io}
        else
          opts = {}
        end
        pid = spawn("#{__dir__}/s3_cleanup.rb", opts)
        log_io.close if log_io
        puts "[cleanup_invoker] spawned pid=#{pid}"
        @lock.synchronize do
          @pid = pid
          @watchdog = start_watchdog(pid)
        end
        @pid
      end

      def finalize_previous_run!
        io = @exitstatus.success? ? $stdout : $stderr
        io.puts "[cleanup_invoker] #{@pid} exited: #{@exitstatus.inspect}"

        @lock.synchronize do
          @exitstatus = nil
          @pid = nil
          @watchdog = nil
        end
      end

      private

      def setup
        @complete_event = SleepyPenguin::EventFD.new(0, :SEMAPHORE)
        @timer = SleepyPenguin::TimerFD.new
        @timer.settime(0, MARGIN, 0)
      end

      def setup_poll
        super
        @poll.add(@timer, [:IN])
        @poll.add(@complete_event, [:IN])
      end

      def terminate
        teardown
        puts "[cleanup_invoker] terminating"
        pid = @lock.synchronize { @pid }
        puts "[cleanup_invoker] SIGTERM => #{pid}"
        begin
          Process.kill(:TERM, pid)
        rescue Errno::ESRCH, Errno::ECHILD
        end
      end

      def main_loop
        puts "[cleanup_invoker] helo"
        catch(:stop) do
          loop do
            @poll.wait do |events, io|
              case io
              when @timer
                io.expirations
                @timer.settime(0, MARGIN, 0)
                perform!
              when @stop_event
                io.value
                throw :stop
              when @complete_event
                io.value
                finalize_previous_run!
              end
            end
          end
        end
        puts "[cleanup_invoker] exiting"
      rescue Exception => e
        $stderr.puts "[cleanup_invoker] !!! encountered error: #{$!.inspect}"
        $!.backtrace.each {|bt| $stderr.puts "[cleanup_invoker]   #{bt}" }
      ensure
        puts "[cleanup_invoker] bye"
        @poll.close
      end

      def start_watchdog(pid)
        @watchdog = Thread.new do
          begin
            puts "[cleanup_invoker/watchdog(#{pid})] grrr..."
            _, status = Process.waitpid2(pid)
            puts "[cleanup_invoker/watchdog(#{pid})] exit #{status.inspect}"
            @exitstatus = status
            @complete_event.incr(1)
          rescue Exception => e
            p $!
          end
        end
      end
    end

    class TimetableUpdater < Worker
      INTERVAL = ENV['AGQR_DEBUG'] == '1' ? 300 : 1800

      def initialize
        @on_update = proc {}
        @timetable = nil
      end

      def on_update(&block)
        @on_update = block
      end

      private

      def setup
        @timer = SleepyPenguin::TimerFD.new
        @timer.settime(0, INTERVAL, 1)
      end

      def setup_poll
        super
        @poll.add(@timer, [:IN])
      end

      def update!
        puts "[timetable_updater] updating..."
        previous_timetable = @timetable
        @timetable = Timetable.streaming
        puts "[timetable_updater] #{@timetable.days.values.flatten.size} programs loaded."
        unless previous_timetable == @timetable
          puts "[timetable_updater] Detect a difference, calling hook"
          @on_update.call @timetable
        end
      rescue Exception => e
        $stderr.puts "[timetable_updater] !!! encountered error: #{$!.inspect}"
        $!.backtrace.each {|bt| $stderr.puts "[timetable_updater]   #{bt}" }
      end

      def main_loop
        puts "[timetable_updater] helo"
        catch(:stop) do
          loop do
            @poll.wait do |events, io|
              case io
              when @timer
                io.expirations
                update!
                val = INTERVAL + rand(120)
                @timer.settime(0, val, val)
                puts "[timetable_updater] next run will after #{val} seconds (#{@timer.gettime.inspect})"
              when @stop_event
                io.value
                throw :stop
              end
            end
          end
        end
        puts "[timetable_updater] exiting"
      ensure
        puts "[timetable_updater] bye"
        @poll.close
      end
    end
  end
end

if $0 == __FILE__
  config_path = "#{__dir__}/config.yml"
  if File.exists?(config_path)
    config = YAML.load_file(config_path)
  else
    config = {}
  end

  Aandg::Scheduler.new(config).run!
end
