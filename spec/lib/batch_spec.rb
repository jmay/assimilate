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
    @batcher = @catalog.start_batch(domain: 'testdata', datestamp: datestamp, idfield: 'ID')

    @records = CSV.read(File.dirname(__FILE__) + "/../data/#{filename}", :headers => true)
    @records.each do |rec|
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
        :adds_count => 5,
        :deletes_count => 0,
        :updates_count => 0,
        :unchanged_count => 0
      }
    end

    it "should load the records verbatim" do
      @catalog.catalog.count.should == @records.count
      example = @records[rand(@records.count)]
      @catalog.where('_resource' => 'testdata', 'ID' => example['ID']).should == example.to_hash
    end

    it "should refuse to do a duplicate import" do
      lambda {import_data("123")}.should raise_error(Assimilate::DuplicateImportError)
    end

    it "should do all no-ops when importing identical data" do
      lambda {import_data("234")}.should_not raise_error
      @batcher.stats.should == {
        :adds_count => 0,
        :deletes_count => 0,
        :updates_count => 0,
        :unchanged_count => 5
      }
      @catalog.catalog.count.should == @records.count
    end
  end

  describe "into existing catalog" do
    before(:all) do
      reset_catalog
      import_data("123")
    end

    before(:each) do
      import_data("345", "updates.csv")
    end

    it "should recognize changes" do
      @batcher.stats.should == {
        :adds_count => 1,
        :deletes_count => 1,
        :updates_count => 1,
        :unchanged_count => 3
      }
      @catalog.active_count.should == @records.count
    end
  end
end
