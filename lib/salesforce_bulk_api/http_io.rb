module SalesforceBulkApi
  require 'http'

  class HttpIo
    private_class_method :new

    def self.open(uri, options = {})
      @io = new
      if block_given?
        begin
          @io.do_open(uri, options)
          return yield(@io)
        ensure
          @io.close
        end
      end
      @io.do_open(uri)
      @io
    end

    def eof?
      @eof
    end

    def close
      @http.close if @http
    end

    def readuntil(terminator)
      begin
        until idx = @buffer.index(terminator)
          buffer_fill
        end
        return buffer_consume(idx + terminator.size)
      rescue EOFError
        @eof = true
        return @buffer.size > 0 ? buffer_consume(@buffer.size) : nil
      end
    end

    def gets(terminator)
      readuntil(terminator)
    end

    def buffer_fill
      case rv = @body.readpartial
        when String
          return @buffer << rv
        else
          # callers do not care about backtrace, so avoid allocating for it
          raise EOFError, 'end of file reached', []
      end
    end

    def buffer_consume(len)
      @buffer.slice!(0, len)
    end

    def do_open(uri, options = {})
      @http = HTTP
                .headers(options.fetch(:headers, {}))
                .timeout(write: 60, connect: 60, read: 60)
                .persistent(uri)
      @body = @http.get(uri).body
      @buffer = ''
      @eof = false
    end
  end
end
