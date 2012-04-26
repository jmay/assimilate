# batch import tests

require "spec_helper"

describe "importing file into empty catalog" do
  before :all do
    assimilator = Assimilate::Catalog.new(:db => 'test')
    @batcher = assimilator.start_batch(resource: 'Affinity', datestamp: '120419', idfield: 'ID')
  end

  it "should load the records verbatim" do
    records = CSV.read(File.dirname(__FILE__) + "/../data/batch_input.csv", :headers => true)
    records.each do |rec|
      @batcher << rec
    end
    @batcher.stats.should == {
      :adds_count => 10,
      :deletes_count => 0,
      :updates_count => 0,
      :unchanged_count => 0
    }
  end
end
