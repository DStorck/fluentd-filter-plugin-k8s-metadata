require 'fluent/filter'

module Fluent
  class KubernetesMetadataFromNamedLogfileFilter < Fluent::Filter
    # Register this filter as "KubernetesMetadataFromNamedLogfile"
    Fluent::Plugin.register_filter('kubernetes_metadata_from_named_logfile', self)

    # config_param works like other plugins

    config_param :logfile, :string, :default => 'our_named_logfile'
    config_param :cache_size, :integer, :default => 100
    config_param :container_name_to_kubernetes_regexp,
                 :string,
                 :default => '^k8s_(?<container_name>[^\.]+)\.[^_]+_(?<pod_name>[^_]+)_(?<namespace>[^_]+)_[^_]+_[a-f0-9]{8}$'
    config_param :tag_to_kubernetes_name_regexp,
                 :string,
                 :default => 'var\.log\.containers\.(?<pod_name>[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*)_(?<namespace>[^_]+)_(?<container_name>.+)-(?<docker_id>[a-z0-9]{64})\.log$'


     def get_metadata(namespace_name, pod_name)
       begin
         metadata = @client.get_pod(pod_name, namespace_name)
         return if !metadata
         labels = syms_to_strs(metadata['metadata']['labels'].to_h)
         annotations = match_annotations(syms_to_strs(metadata['metadata']['annotations'].to_h))
         kubernetes_metadata = {
             'namespace_name' => namespace_name,
             'pod_id'         => metadata['metadata']['uid'],
             'pod_name'       => pod_name,
             'labels'         => labels,
             'host'           => metadata['spec']['nodeName']
         }
         kubernetes_metadata['annotations'] = annotations unless annotations.empty?
         return kubernetes_metadata
       rescue KubeException
         nil
       end
     end

    def initialize
     super
    end

    def configure(conf)
      super
      @tag_to_kubernetes_name_regexp_compiled = Regexp.compile(@tag_to_kubernetes_name_regexp)
      @container_name_to_kubernetes_regexp_compiled = Regexp.compile(@container_name_to_kubernetes_regexp)
    end


    def filter_stream_from_files(tag, es)
         new_es = MultiEventStream.new

         match_data = tag.match(@tag_to_kubernetes_name_regexp_compiled)

         if match_data
           metadata = {
             'logfile' => "name of logfile", #match_data['logfile']
              #  'container_id' => match_data['docker_id']

             'kubernetes' => {
               'namespace_name' => match_data['namespace'],
               'pod_name'       => match_data['pod_name'],
             }
           }

           if @kubernetes_url.present?
             cache_key = "#{metadata['kubernetes']['namespace_name']}_#{metadata['kubernetes']['pod_name']}"

             this     = self
             metadata = @cache.getset(cache_key) {
               if metadata
                 kubernetes_metadata = this.get_metadata(
                   metadata['kubernetes']['namespace_name'],
                   metadata['kubernetes']['pod_name']
                 )
                 metadata['kubernetes'] = kubernetes_metadata if kubernetes_metadata
                 metadata
               end
             }
             if @include_namespace_id
               namespace_name = metadata['kubernetes']['namespace_name']
               namespace_id = @namespace_cache.getset(namespace_name) {
                 namespace = @client.get_namespace(namespace_name)
                 namespace['metadata']['uid'] if namespace
               }
               metadata['kubernetes']['namespace_id'] = namespace_id if namespace_id
             end
           end

          #  metadata['kubernetes']['container_name'] = match_data['container_name']
         end

         es.each { |time, record|
          #  record = merge_json_log(record) if @merge_json_log

           record = record.merge(metadata) if metadata

           new_es.add(time, record)
         }

         new_es
       end
    end


  end
end
