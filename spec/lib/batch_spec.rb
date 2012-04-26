# batch import tests

require "spec_helper"

describe "importing file into empty catalog" do
  before :all do
    @catalog = Assimilate::Catalog.new(:config => File.dirname(__FILE__) + "/../data/test.yml")

    @catalog.catalog.remove
    @catalog.batches.remove

    @batcher = @catalog.start_batch(domain: 'testdata', datestamp: '120419', idfield: 'ID')
  end

  it "should load the records verbatim" do
    records = CSV.read(File.dirname(__FILE__) + "/../data/batch_input.csv", :headers => true)
    records.each do |rec|
      @batcher << rec
    end
    @batcher.stats.should == {
      :adds_count => 5,
      :deletes_count => 0,
      :updates_count => 0,
      :unchanged_count => 0
    }
    @batcher.commit
    example = records[rand(records.count)]
    @catalog.where('_resource' => 'testdata', 'ID' => example['ID']).should == example.to_hash
  end
end
