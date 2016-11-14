# -*- encoding: utf-8 -*-
require File.expand_path('../lib/assimilate/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Jason May"]
  gem.email         = ["jmay@pobox.com"]
  gem.description   = %q{Ingest updates from CSV and apply to set of hashes}
  gem.summary       = %q{Review & incorporate changes to a repository of persistent hashes in mongodb.}
  gem.homepage      = ""
  gem.rubyforge_project = "assimilate"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "assimilate"
  gem.require_paths = ["lib"]
  gem.version       = Assimilate::VERSION

  gem.required_ruby_version = '>= 2.3.1'

  gem.add_dependency "mongo", "~> 2.3.0"
  #gem.add_dependency "bson_ext", "~> 1.12.0"
  gem.add_dependency 'activesupport', "~> 3.2.0"

  gem.add_development_dependency "rake", "~> 11.2"
  gem.add_development_dependency "rspec", "~> 3.5.0"
  #gem.add_development_dependency "guard-rspec", "~> 0.7.0"
  #gem.add_development_dependency "ruby_gntp", "~> 0.3.4"
end
