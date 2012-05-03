# tests for extending the base records

require "spec_helper"

describe "loading extended data" do
  before(:all) do
    @catalog = Assimilate::Catalog.new(:config => File.dirname(__FILE__) + "/../data/test.yml")
    reset_catalog
  end

  def reset_catalog
    @catalog.catalog.remove
    @catalog.batches.remove
  end

  def import_base_data(datestamp, filename = "batch_input.csv")
    @batcher = @catalog.start_batch(domain: 'testdata', datestamp: datestamp, idfield: 'ID')

    Assimilate.slurp(File.dirname(__FILE__) + "/../data/#{filename}") do |rec|
      @batcher << rec
    end
    @batcher.commit
  end

  describe "into matching catalog entries" do
    before(:all) do
      reset_catalog
      import_base_data("123")
    end

    def import_extended_data(datestamp, filename)
      @extender = @catalog.extend_data(domain: 'testdata', datastamp: datestamp, idfield: 'ID', key: 'inauguration')
      Assimilate.slurp(File.dirname(__FILE__) + "/../data/#{filename}") do |rec|
        @extender << rec
      end
      @extender.commit
    end

    before(:each) do
      import_extended_data("1001", "dates.csv")
    end

    it "should capture changes" do
      @extender.stats.should == {
        :baseline_count => 6,
        :final_count => 7,
        :distinct_ids => 4,
        :adds_count => 1,
        :new_ids => ['16'],
        :updates_count => 3,
        :updated_fields => {'date' => 4},
        :unchanged_count => 0
      }
    end

    it "should do no-ops on duplicate load" do
      # import_extended_data("1002", "dates")
      lambda {import_extended_data("1002", "dates.csv")}.should_not raise_error

      @extender.stats.should == {
        :baseline_count => 7,
        :final_count => 7,
        :distinct_ids => 4,
        :adds_count => 0,
        :new_ids => [],
        :updates_count => 0,
        :updated_fields => {},
        :unchanged_count => 4
      }
    end
  end

  # test handling of multiple records for same ID in the extended-data file
  # test importing data at top level (no keyfield for sub-attributes)
end
