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
        super([@endpoint], *options)
         
        refresh   
      end

      def refresh
        @refreshed_at = Time.now
        unless @refresher && @refresher.alive?
          @refresher = Thread.new {check_version; @refresher = nil}
        end
      end

      private 


      #version checking can block if the elasticache node is down.
      def check_version
        begin
          return unless elasticache
          elasticache.refresh
          if @version < elasticache.version
            Rails.logger.info "Refreshing dalli-elasticache servers. New servers: #{@elasticache.servers}, version: #{@elasticache.version}"
            @version = elasticache.version
            if @pool_options.empty?
              @data = Dalli::Client.new(elasticache.servers, @options)
            else
              @data = ::ConnectionPool.new(@pool_options.dup) { Dalli::Client.new(elasticache.servers, @options.merge(:threadsafe => false)) }
            end
          end
        rescue => e
          Airbrake.notify(e) if defined? Airbrake
          Rails.logger.info "rescued #{e} trying to refresh elasticache"
        end
      end

      def elasticache
        if @elasticache.nil?
          @elasticache = Dalli::ElastiCache.new(@endpoint) 
          @version = @elasticache.version
        end
        @elasticache
      rescue => e
        Rails.logger.info "Failed to fetch elasticache info: #{e.message}"
        nil
      end
    end
  end
end

