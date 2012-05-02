class Assimilate::Extender
  attr_reader :domain, :idfield, :keyfield

  def initialize(args)
    @catalog = args[:catalog]
    @domain = args[:domain]
    @idfield = args[:idfield]
    @filename = args[:filename]
    @keyfield = args[:key]

    load_baseline

    @noops = []
    @changes = []
    @adds = []
    @deletes = []
  end

  def load_baseline
    stored_records = @catalog.catalog.find(@catalog.domainkey => @domain).to_a
    @baseline = stored_records.each_with_object({}) do |rec, h|
      key = rec[@idfield]
      if h.include?(key)
        raise Assimilate::CorruptDataError, "Duplicate records for key [#{key}] in domain [#{@domain}]"
      end
      h[key] = rec
    end
  end

  def <<(record)
    @seen ||= Hash.new(0)

    hash = record.to_hash
    key = hash[@idfield]
    data = hash.reject {|k,v| k == idfield}
    # @seen[key] = data
    current_record = @baseline[key]
    if current_record
      if current_record[@keyfield] == data
        @noops << key
        @seen[key] = {}
      else
        @changes << key
        @seen[key] = data
      end
    else
      @adds << key
      @seen[key] = data
    end
  end

  def stats
    {
      :baseline_count => @baseline.size,
      :final_count => @baseline.size + @adds.count,
      :distinct_ids => @seen.size,
      :adds_count => @adds.count,
      :new_ids => @adds,
      :updates_count => @changes.count,
      :updated_fields => @seen.each_with_object(Hash.new(0)) {|(k,hash),memo| hash.each {|k,v| memo[k] += 1}},
      :unchanged_count => @noops.count
    }
  end

  # write all the changes to the catalog
  def commit
    apply_inserts
    apply_updates
  end

  # an "insert" here means a record for which we have extended data
  # but does not appear in the current catalog, so we need to create
  # a stub entry.
  def apply_inserts
    @adds.each do |key|
      data = @seen[key]
      @catalog.catalog.insert(
        @catalog.domainkey => domain,
        idfield => key,
        keyfield => data
      )
    end
  end

  # "update" means store the extended data in the record (which must exist)
  def apply_updates
    @changes.each do |key|
      data = @seen[key]
      @catalog.catalog.update(
        {
          @catalog.domainkey => domain,
          idfield => key
        },
        {"$set" => {
            keyfield => data
          }
        }
      )
    end
  end


end
