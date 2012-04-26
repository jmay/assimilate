class Assimilate::Batch
  def initialize(args)
    @baseline = {}

    @noops = []
    @changes = []
    @adds = []
    @deletes = []
  end

  def <<(hash)
    puts hash.inspect
    key = hash[@idfield]
    if current = @baseline[key]
      if current == hash
        @noops << hash
      else
        @changes << hash
      end
    else
      @adds << hash
    end
  end

  # def commit
    
  # end

  def stats
    {
      :adds_count => @adds.count,
      :deletes_count => @deletes.count,
      :updates_count => @updates.count,
      :unchanged_count => @noops.count
    }
  end
end
