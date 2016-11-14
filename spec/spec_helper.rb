#require 'rspec/autorun'
require "tempfile"
require "csv"

require File.expand_path('../../lib/assimilate', __FILE__)

RSpec.configure do |config|
  config.expect_with(:rspec) { |c| c.syntax = :should }
end
