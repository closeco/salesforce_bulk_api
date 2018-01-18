module SalesforceBulkApi
  require 'salesforce_bulk_api/http_io'

  class Job
    attr_reader :job_id
    attr_reader :batch_ids

    XML_HEADER = '<?xml version="1.0" encoding="utf-8" ?>'
    MAX_BATCH_SIZE = 10_000

    def initialize(args)
      @job_id         = args[:job_id]
      @operation      = args[:operation]
      @sobject        = args[:sobject]
      @external_field = args[:external_field]
      @connection     = args[:connection]
      @batch_ids      = []
      @nullable_fields = args[:nullable_fields] || []
    end

    def create_job(options = {})
      @pk_chunking = options.fetch(:pk_chunking, false)
      @concurrency_mode = options.fetch(:concurrency_mode, 'Parallel')

      if @concurrency_mode != 'Parallel' && @concurrency_mode != 'Serial'
        raise "Unexpected concurrency mode #{@concurrency_mode}, expected 'Parallel', 'Serial'"
      end

      xml = "#{XML_HEADER}<jobInfo xmlns=\"http://www.force.com/2009/06/asyncapi/dataload\">"
      xml += "<operation>#{@operation}</operation>"
      xml += "<object>#{@sobject}</object>"
      # This only happens on upsert
      if !@external_field.nil? && @operation == 'upsert'
        xml += "<externalIdFieldName>#{@external_field}</externalIdFieldName>"
      end
      xml += "<concurrencyMode>#{@concurrency_mode}</concurrencyMode>"
      xml += '<contentType>CSV</contentType>'
      xml += '</jobInfo>'
      path = 'job'
      headers = Hash['Content-Type' => 'application/xml; charset=utf-8']

      if @pk_chunking
        headers['Sforce-Enable-PKChunking'] = 'true'
      end

      response = @connection.post_request(nil, path, xml, headers)
      response_parsed = XmlSimple.xml_in(response)

      raise SalesforceBulkApi::SalesforceException.new(
        "#{response_parsed['exceptionMessage'][0]} (#{response_parsed['exceptionCode'][0]})"
      ) if response_parsed['exceptionCode']


      @job_id = response_parsed['id'][0]
    end

    def close_job
      xml = "#{XML_HEADER}<jobInfo xmlns=\"http://www.force.com/2009/06/asyncapi/dataload\">"
      xml += '<state>Closed</state>'
      xml += '</jobInfo>'

      path = "job/#{@job_id}"
      headers = Hash['Content-Type' => 'application/xml; charset=utf-8']

      response = @connection.post_request(nil, path, xml, headers)
      XmlSimple.xml_in(response)
    end

    def add_query(query)
      path = "job/#{@job_id}/batch/"
      content_type = 'text/csv'
      headers = Hash['Content-Type' => "#{content_type}; charset=UTF-8"]

      response = @connection.post_request(nil, path, query, headers)
      response_parsed = XmlSimple.xml_in(response)

      @batch_ids << response_parsed['id'][0]
    end

    def add_batches(records, batch_size = MAX_BATCH_SIZE)
      raise 'Records must be an array of hashes.' unless records.is_a? Array
      unless 0 < batch_size && batch_size <= MAX_BATCH_SIZE
        raise "Invalid batch size #{batch_size}, expected 1 .. #{MAX_BATCH_SIZE}"
      end

      keys = records
        .inject(Set.new) { |set, record| set.merge(record.keys); set }
        .to_a

      records_dup = records.clone

      super_records = []
      (records_dup.size / batch_size).to_i.times do
        super_records << records_dup.pop(batch_size)
      end
      super_records << records_dup unless records_dup.empty?

      super_records.each do |batch|
        @batch_ids << add_batch(keys, batch)
      end
      @batch_ids
    end

    def check_job_status
      path = "job/#{@job_id}"
      headers = Hash.new
      response = @connection.get_request(nil, path, headers)
      XmlSimple.xml_in(response) if response
    end

    def check_batch_status(batch_id=nil)
      path = "job/#{@job_id}/batch"
      path += "/#{batch_id}" unless batch_id.nil?
      headers = Hash.new
      response = @connection.get_request(nil, path, headers)
      XmlSimple.xml_in(response) if response
    end

    def get_job_result(return_result, timeout)
      # timeout is in seconds
      begin
        state = []
        Timeout::timeout(timeout, SalesforceBulkApi::JobTimeout) do
          while true
            if self.check_job_status['state'][0] == 'Closed'
              batch_statuses = {}

              batches_ready = @batch_ids.all? do |batch_id|
                batch_state = batch_statuses[batch_id] = self.check_batch_status(batch_id)
                batch_state['state'][0] != 'Queued' && batch_state['state'][0] != 'InProgress'
              end

              if batches_ready
                @batch_ids.each do |batch_id|
                  state.insert(0, batch_statuses[batch_id])
                  @batch_ids.delete(batch_id)
                end
              end
              break if @batch_ids.empty?
            else
              break
            end
          end
        end
      rescue SalesforceBulkApi::JobTimeout
        raise "Timeout waiting for Salesforce to process job batches #{@batch_ids} of job #{@job_id}."
      end

      state.each_with_index do |batch_state, i|
        if batch_state['state'][0] == 'Completed' && return_result == true
          state[i].merge!({
            'response' => self.get_batch_result(batch_state['id'][0])
          })
        end
      end
      state
    end

    def get_batch_result(batch_id)
      if @operation == 'query'
        self.results(batch_id).to_a
      else
        path = "job/#{@job_id}/batch/#{batch_id}/result"
        headers = Hash['Content-Type' => 'application/xml; charset=UTF-8']
        response = @connection.get_request(nil, path, headers) || ''
        CSV.parse(
          response,
          encoding: 'utf-8',
          row_sep: "\n",
          col_sep: ',',
          headers: :first_row,
          header_converters: lambda { |h| h.downcase }
        ).map(&:to_hash)
      end
    end

    def results(batch_id)
      path = "job/#{@job_id}/batch/#{batch_id}/result"
      headers = Hash['Content-Type' => 'application/xml; charset=UTF-8']

      response = @connection.get_request(nil, path, headers)
      response_parsed = XmlSimple.xml_in(response)

      Enumerator.new do |yielder|

        (response_parsed['result'] || []).each do |result_id|
          path = "job/#{@job_id}/batch/#{batch_id}/result/#{result_id}"
          uri = "https://#{@connection.instance_host}#{@connection.path_prefix}#{path}"

          headers['Content-Type'] = 'text/csv'
          headers['X-SFDC-Session'] = @connection.session_id

          SalesforceBulkApi::HttpIo.open(uri, {headers: headers}) do |io|
            CSV.new(io, encoding: 'utf-8', row_sep: "\n", col_sep: ',', headers: :first_row).each do |csv|
              yielder << csv.to_hash
            end
          end
        end
      end
    end

    private

    def add_batch(keys, batch)
      text = CSV.generate({
        write_headers: true,
        encoding: 'utf-8',
        row_sep: "\n",
        col_sep: ',',
        headers: keys
      }) do |csv|
        batch.each { |object| csv << create_sobject(keys, object) }
      end

      path = "job/#{@job_id}/batch/"
      headers = Hash['Content-Type' => 'text/csv; charset=UTF-8']
      response = @connection.post_request(nil, path, text, headers)
      response_parsed = XmlSimple.xml_in(response) || {}

      raise SalesforceBulkApi::ProcessingException.new(
        "Failed to create a new batch, Salesforce response: #{response.to_s}," +
          "job id #{@job_id}, batch size: #{batch.size} objects, #{text.length} symbols"
      ) if (response_parsed['id'] || []).empty?

      response_parsed['id'][0]
    end

    def create_sobject(keys, r)
      result = []
      keys.each do |k|
        value = r[k]
        if value.to_s.empty? && !@nullable_fields.include?(k.to_s)
          raise SalesforceBulkApi::ProcessingException.new(
            "Value is empty or not specified #{k}, #{r.to_s}, use space instead, nullable field: #{@nullable_fields}",
          )
        elsif value.is_a?(Hash)
          raise SalesforceBulkApi::ProcessingException.new("Unsupported field type, #{k}, #{r.to_s}")
        else
          if value.respond_to?(:iso8601) # timestamps
            result << value.iso8601.to_s
          else
            result << (value.nil? ? '#N/A' : value.to_s)
          end
        end
      end
      result
    end
  end

  class JobTimeout < StandardError
  end

  class SalesforceException < StandardError
  end

  class ProcessingException < StandardError
  end

end
