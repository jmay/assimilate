require "optparse"

class Assimilate::Command
  attr_reader :command, :options

  def initialize
    @options = {}
    @parser = OptionParser.new do |opts|
      opts.banner = "Usage: assimilate [command] [options]"

      opts.on("--config FILENAME", String, "Catalog database configuration file") do |f|
        @options[:config] = f
      end

      opts.on("--id FIELDNAME", String, "Field name to be used for record identifier") do |f|
        @options[:idfield] = f
      end
    end
  end

  def parse(argv = ARGV)
    @command = argv.shift
    filenames = @parser.parse(argv)

    raise OptionParser::MissingArgument, "missing config" unless options[:config]
    raise OptionParser::MissingArgument, "missing idfield" unless options[:idfield]
    raise "missing filename" unless filenames.any?

    # argv remnants are filenames
    [@command, @options, filenames]
  end

  def execute(command, options, filenames = nil)
    filename = filenames.first

    case command
    when 'load'
      results = Assimilate.load(filename, options)
      logmessage(command, options, results)

    else
      raise "unknown command #{command}"
    end
  end

  def logmessage(command, options, results)
    $stderr.puts <<-EOT
* assimilate #{command} (#{options.keys.join(', ')})
EOT
    $stderr.puts results.inspect
  end
end
