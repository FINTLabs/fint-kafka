package no.novari.kafka.consumertracking.event.report;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ReportMappingService {

    public <VALUE> Map<TopicPartitionReport, List<KeyValueReport<VALUE>>> toKeyValueReportsPerTopicPartition(
            ConsumerRecords<String, VALUE> records
    ) {
        return records
                .partitions()
                .stream()
                .collect(Collectors.toMap(
                        this::toTopicPartitionReport,
                        topicPartition -> toKeyValueReports(records.records(topicPartition))
                ));
    }

    public <VALUE> List<KeyValueReport<VALUE>> toKeyValueReports(Collection<ConsumerRecord<String, VALUE>> records) {
        return records
                .stream()
                .map(this::toKeyValueReport)
                .collect(Collectors.toList());
    }

    public <VALUE> KeyValueReport<VALUE> toKeyValueReport(ConsumerRecord<String, VALUE> record) {
        return new KeyValueReport<>(record.key(), record.value());
    }

    public TopicPartitionReport toTopicPartition(ConsumerRecord<?, ?> record) {
        return new TopicPartitionReport(record.topic(), record.partition());
    }

    public ExceptionReport toExceptionReport(Exception exception) {
        if (exception instanceof ListenerExecutionFailedException) {
            Exception cause = (Exception) exception.getCause();
            return new ExceptionReport(cause.getClass(), cause.getMessage());
        }
        return new ExceptionReport(
                exception.getClass(),
                exception.getMessage()
        );
    }

    public Map<TopicPartitionReport, Long> toTopicPartitionAssignments(Map<TopicPartition, Long> assignments) {
        return assignments
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        entry -> toTopicPartitionReport(entry.getKey()),
                        Map.Entry::getValue
                ));
    }

    public Set<TopicPartitionReport> toTopicPartitionReports(Collection<TopicPartition> partitions) {
        return partitions
                .stream()
                .map(this::toTopicPartitionReport)
                .collect(Collectors.toSet());
    }

    public TopicPartitionReport toTopicPartitionReport(TopicPartition partition) {
        return new TopicPartitionReport(partition.topic(), partition.partition());
    }

}
