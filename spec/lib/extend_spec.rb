# tests for extending the base records

require "spec_helper"

describe "loading extended data" do
  before(:all) do
    @catalog = Assimilate::Catalog.new(:config => File.dirname(__FILE__) + "/../data/test.yml")
    reset_catalog
  end

  def reset_catalog
    @catalog.catalog.drop
    @catalog.batches.drop
  end

  def import_base_data(datestamp, filename = "batch_input.csv")
    @batcher = @catalog.start_batch(domain: 'testdata', datestamp: datestamp, idfield: 'ID')

    Assimilate.slurp(File.dirname(__FILE__) + "/../data/#{filename}") do |rec|
      @batcher << rec
    end
    @batcher.commit
  end

  def import_extended_data(datestamp, filename, opts = {})
    @extender = @catalog.extend_data(opts.merge(domain: 'testdata', datestamp: datestamp, idfield: 'ID', key: 'inauguration'))
    Assimilate.slurp(File.dirname(__FILE__) + "/../data/#{filename}") do |rec|
      @extender << rec
    end
    @extender.commit
  end

  def import_toplevel_extended_data(datestamp, filename, opts = {})
    @extender = @catalog.extend_data(opts.merge(domain: 'testdata', datestamp: datestamp, idfield: 'ID'))
    Assimilate.slurp(File.dirname(__FILE__) + "/../data/#{filename}") do |rec|
      @extender << rec
    end
    @extender.commit
  end

  describe "into matching catalog entries" do
    before(:all) do
      reset_catalog
      import_base_data("123")
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

  describe "at top level of catalog entries" do
    before(:all) do
      reset_catalog
      import_base_data("123")
    end

    before(:each) do
      import_toplevel_extended_data("991", "birthdates.csv")
    end

    it "should capture changes" do
      @extender.stats.should == {
        :baseline_count => 6,
        :final_count => 7,
        :distinct_ids => 4,
        :adds_count => 1,
        :new_ids => ['999'],
        :updates_count => 3,
        :updated_fields => {'birthdate' => 4},
        :unchanged_count => 0
      }
    end

    it "should do no-ops on duplicate load" do
      # import_extended_data("1002", "dates")
      lambda {import_toplevel_extended_data("992", "birthdates.csv")}.should_not raise_error

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

  describe "updating log entries" do
    before(:all) do
      reset_catalog
      import_base_data("20120501")
      import_extended_data("20120505", "logs1.csv", :compare => 'timestamp')
    end


    before(:each) do
    end

    it "should capture changes" do
      @extender.stats.should == {
        :baseline_count => 6,
        :final_count => 6,
        :distinct_ids => 4,
        :adds_count => 0,
        :new_ids => [],
        :updates_count => 4,
        :updated_fields => {'timestamp' => 4, 'event' => 4},
        :unchanged_count => 0
      }
    end

    it "should load the new events" do
      lambda {import_extended_data("20120506", "logs2.csv", :compare => 'timestamp')}.should_not raise_error

      @extender.stats.should == {
        :baseline_count => 6,
        :final_count => 6,
        :distinct_ids => 4,
        :adds_count => 0,
        :new_ids => [],
        :updates_count => 4,
        :updated_fields => {'timestamp' => 4, 'event' => 4},
        :unchanged_count => 0
      }
    end
  end

  describe "updating log entries in reverse order" do
    before(:all) do
      reset_catalog
      import_base_data("20120501")
      import_extended_data("20120505", "logs2.csv")
    end


    before(:each) do
    end

    it "should capture changes" do
      @extender.stats.should == {
        :baseline_count => 6,
        :final_count => 6,
        :distinct_ids => 4,
        :adds_count => 0,
        :new_ids => [],
        :updates_count => 4,
        :updated_fields => {'timestamp' => 4, 'event' => 4},
        :unchanged_count => 0
      }
    end

    it "should load the new events" do
      lambda {import_extended_data("20120506", "logs1.csv", :compare => 'timestamp')}.should_not raise_error

      @extender.stats.should == {
        :baseline_count => 6,
        :final_count => 6,
        :distinct_ids => 4,
        :adds_count => 0,
        :new_ids => [],
        :updates_count => 0,
        :updated_fields => {},
        :unchanged_count => 4
      }
    end
  end

  describe "with incorrect options" do
    it "should reject if can't find any records to extend" do
      lambda {
        @catalog.extend_data(domain: 'testdata', idfield: 'missingkey', key: 'inauguration')
        # Assimilate.slurp(File.dirname(__FILE__) + "/../data/#{filename}") do |rec|
        #   @extender << rec
        # end
        # @extender.commit
      }.should raise_error(Assimilate::CorruptDataError, "Unable to find any records with missingkey in _resource [testdata]")
    end
  end

  describe "with conflicting source records" do
    before(:all) do
      reset_catalog
      import_base_data("123", "master_records_conflicting.csv")
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
  end

  # test handling of multiple records for same ID in the extended-data file
  # test importing data at top level (no keyfield for sub-attributes)
end
