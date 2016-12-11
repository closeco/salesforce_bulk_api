require 'rubygems'
require 'bundler/setup'
require 'salesforce_bulk_api'

RSpec.configure do |config|

  config.filter_run :focus => true
  config.run_all_when_everything_filtered = true
  config.expect_with(:rspec) { |c| c.syntax = :should }

end
