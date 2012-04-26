# batch import tests

require "spec_helper"

describe "importing file into empty catalog" do
  def import_data
    @records = CSV.read(File.dirname(__FILE__) + "/../data/batch_input.csv", :headers => true)
    @records.each do |rec|
      @batcher << rec
    end
    @batcher.commit
  end

  before :each do
    @catalog = Assimilate::Catalog.new(:config => File.dirname(__FILE__) + "/../data/test.yml")

    # start with a blank repo
    @catalog.catalog.remove
    @catalog.batches.remove

    @batcher = @catalog.start_batch(domain: 'testdata', datestamp: '120419', idfield: 'ID')

    import_data
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
    lambda {import_data}.should raise_error(Assimilate::DuplicateImportError)
  end

  it "should do all no-ops when importing identical data" do
    @batcher = @catalog.start_batch(domain: 'testdata', datestamp: '120420', idfield: 'ID')
    lambda {import_data}.should_not raise_error
    @batcher.stats.should == {
      :adds_count => 0,
      :deletes_count => 0,
      :updates_count => 0,
      :unchanged_count => 5
    }
    @catalog.catalog.count.should == @records.count
  end
end
