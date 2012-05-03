# batch import tests

require "spec_helper"

describe "importing file" do
  before(:all) do
    @catalog = Assimilate::Catalog.new(:config => File.dirname(__FILE__) + "/../data/test.yml")
    reset_catalog
  end

  def reset_catalog
    @catalog.catalog.remove
    @catalog.batches.remove
  end

  def import_data(datestamp, filename = "batch_input.csv")
    @batcher = @catalog.start_batch(domain: 'testdata', datestamp: datestamp, filename: filename, idfield: 'ID')

    Assimilate.slurp(File.dirname(__FILE__) + "/../data/#{filename}") do |rec|
      @batcher << rec
    end
    @batcher.commit
  end

  describe "into empty catalog" do
    before :each do
      reset_catalog
      import_data("123")
    end

    it "should return correct import stats" do
      @batcher.stats.should == {
        :baseline_count => 0,
        :final_count => 6,
        :adds_count => 6,
        :new_ids => ["1", "2", "3", "4", "5", "6"],
        :deletes_count => 0,
        :deleted_ids => [],
        :updates_count => 0,
        :updated_ids => [],
        :unchanged_count => 0,
        :updated_fields => {}
      }
    end

    it "should load the records verbatim" do
      @catalog.catalog.count.should == 6
      franklin = @catalog.where('_resource' => 'testdata', 'ID' => '3')
      franklin.keys.sort.should == ['ID', 'name', 'title', 'spouse', '_id', '_resource', '_first_seen'].sort
    end

    it "should refuse to do a duplicate import" do
      lambda {import_data("123")}.should raise_error(Assimilate::DuplicateImportError, "duplicate batch for datestamp 123")
    end

    it "should refuse to re-import same file" do
      lambda {import_data("234")}.should raise_error(Assimilate::DuplicateImportError, "duplicate batch for file batch_input.csv")
    end

    it "should do all no-ops when importing identical data" do
      lambda {import_data("234", "duplicate_input.csv")}.should_not raise_error
      @batcher.stats.should == {
        :baseline_count => 6,
        :final_count => 6,
        :adds_count => 0,
        :new_ids => [],
        :deletes_count => 0,
        :deleted_ids => [],
        :updates_count => 0,
        :updated_ids => [],
        :unchanged_count => 6,
        :updated_fields => {}
      }
      @catalog.catalog.count.should == 6
    end
  end

  describe "into existing catalog" do
    before(:all) do
      reset_catalog
      import_data("123")

      import_data("345", "updates.csv")
    end

    it "should recognize changes" do
      @batcher.stats.should == {
        :baseline_count => 6,
        :final_count => 7,
        :adds_count => 1,
        :new_ids => ["7"],
        :deletes_count => 2,
        :deleted_ids => ['4', '6'],
        :updates_count => 1,
        :updated_ids => ['3'],
        :unchanged_count => 3,
        :updated_fields => {'title' => 1, 'spouse' => 1}
      }
      @catalog.active_count.should == 5
    end

    it "should handle deleted attributes" do
      franklin = @catalog.where('ID' => '3')
      franklin['spouse'].should be_nil
      franklin['_last_updated'].should == '345'
    end
  end
end
