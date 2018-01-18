require 'rubygems'
require 'bundler'
require 'net/https'
require 'xmlsimple'
require 'csv'

require 'salesforce_bulk_api/version'
require 'salesforce_bulk_api/concerns/throttling'
require 'salesforce_bulk_api/job'
require 'salesforce_bulk_api/connection'

module SalesforceBulkApi
  class Api
    attr_reader :connection

    SALESFORCE_API_VERSION = '32.0'

    def initialize(client)
      @connection = SalesforceBulkApi::Connection.new(SALESFORCE_API_VERSION, client)
      @listeners = { job_created: [] }
    end

    def upsert(sobject, records, options = {})
      do_operation('upsert', sobject, records, options = options)
    end

    def update(sobject, records, options = {})
      do_operation('update', sobject, records, options = options)
    end

    def create(sobject, records, options = {})
      do_operation('insert', sobject, records, options = options)
    end

    def delete(sobject, records, options = {})
      do_operation('delete', sobject, records, options = options)
    end

    def query(sobject, query, options = {})
      get_response = options.fetch(:get_response, true)
      args = options.dup
      args[:get_response] = get_response
      do_operation('query', sobject, query, options = args)
    end

    def counters
      {
        http_get: @connection.counters[:get],
        http_post: @connection.counters[:post],
        upsert: get_counters[:upsert],
        update: get_counters[:update],
        create: get_counters[:create],
        delete: get_counters[:delete],
        query: get_counters[:query]
      }
    end

    ##
    # Allows you to attach a listener that accepts the created job (which has a useful #job_id field).  This is useful
    # for recording a job ID persistently before you begin batch work (i.e. start modifying the salesforce database),
    # so if the load process you are writing needs to recover, it can be aware of previous jobs it started and wait
    # for them to finish.
    def on_job_created(&block)
      @listeners[:job_created] << block
    end

    def job_from_id(job_id, options = {})
      options[:job_id] = job_id
      options[:connection] = @connection
      SalesforceBulkApi::Job.new(options)
    end

    def do_operation(operation, sobject, workload, options = {})
      close_job = options.fetch(:close_job, true)
      pk_chunking = options.fetch(:pk_chunking, false)
      get_response = options.fetch(:get_response, false)
      timeout = options.fetch(:timeout, 1500)
      batch_size = options.fetch(:batch_size, SalesforceBulkApi::Job::MAX_BATCH_SIZE)

      count operation.to_sym

      job = SalesforceBulkApi::Job.new(
          operation: operation,
          sobject: sobject,
          external_field: options[:external_field],
          connection: @connection,
          nullable_fields: options[:nullable_fields]
      )

      job.create_job(options)
      @listeners[:job_created].each { |callback| callback.call(job) }
      operation == 'query' ? job.add_query(workload) : job.add_batches(workload, batch_size)
      response = (close_job && !pk_chunking) ? job.close_job : {}
      response.merge!({
        'batches' => job.get_job_result(get_response, timeout)
      }) if get_response == true
      response
    end

    private

    def get_counters
      @counters ||= Hash.new(0)
    end

    def count(name)
      get_counters[name] += 1
    end

  end
end
