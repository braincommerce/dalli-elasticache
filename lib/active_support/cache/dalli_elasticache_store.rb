require 'dalli-elasticache'
require 'active_support/cache/dalli_store'

module ActiveSupport
  module Cache
    class DalliElasticacheStore < DalliStore
      attr_reader :refreshed_at

      def initialize(*endpoint_and_options)
        @endpoint, *options = endpoint_and_options
        

        @pool_options = {}
        if dalli_options = options.last && dalli_options.is_a?(Hash)
          @pool_options[:size] = dalli_options[:pool_size] if dalli_options[:pool_size]
          @pool_options[:timeout] = dalli_options[:pool_timeout] if dalli_options[:pool_timeout]
        end
        @refreshed_at = Time.now
        if elasticache
          super(elasticache.servers, *options)
        else
          super([@endpoint], *options)
        end
      end

      def refresh
        @refreshed_at = Time.now
        return unless elasticache

        old_version = elasticache.version
        elasticache.refresh
        if old_version < elasticache.version
          Rails.logger.info "Refreshing dalli-elasticache servers. New servers: #{@elasticache.servers}, version: #{@elasticache.version}"
          if @pool_options.empty?
            @data = Dalli::Client.new(elasticache.servers, @options)
          else
            @data = ::ConnectionPool.new(@pool_options.dup) { Dalli::Client.new(elasticache.servers, @options.merge(:threadsafe => false)) }
          end
        end
      end

      private 

      def elasticache
        @elasticache ||= Dalli::ElastiCache.new(@endpoint)
      rescue => e
        Rails.logger.info "Failed to fetch elasticache info: #{e.message}"
        nil
      end
    end
  end
end

