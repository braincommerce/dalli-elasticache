require 'dalli-elasticache'
require 'active_support/cache/dalli_store'

module ActiveSupport
  module Cache
    class DalliElasticacheStore < DalliStore
      attr_reader :refreshed_at

      def initialize(*endpoint_and_options)
        endpoint, *options = endpoint_and_options
        @elasticache = Dalli::ElastiCache.new(endpoint)

        @pool_options = {}
        if dalli_options = options.last && dalli_options.is_a?(Hash)
          @pool_options[:size] = dalli_options[:pool_size] if dalli_options[:pool_size]
          @pool_options[:timeout] = dalli_options[:pool_timeout] if dalli_options[:pool_timeout]
        end
        @refreshed_at = Time.now
        super(@elasticache.servers, options)
      end

      def refresh
        old_version = @elasticache.version
        @elasticache.refresh
        @refreshed_at = Time.now
        if old_version < @elasticache.version
          Rails.logger.info "Refreshing dalli-elasticache servers. New servers: #{@elasticache.servers}, version: #{@elasticache.version}"
          if @pool_options.empty?
            @data = Dalli::Client.new(@elasticache.servers, @options)
          else
            @data = ::ConnectionPool.new(@pool_options.dup) { Dalli::Client.new(@elasticache.servers, @options.merge(:threadsafe => false)) }
          end
        end
      end
    end
  end
end

