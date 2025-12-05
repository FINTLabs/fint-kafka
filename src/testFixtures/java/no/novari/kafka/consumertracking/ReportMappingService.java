package no.novari.kafka.consumertracking;

import no.novari.kafka.consumertracking.event.reports.ExceptionReport;
import no.novari.kafka.consumertracking.event.reports.KeyValueReport;
import no.novari.kafka.consumertracking.event.reports.TopicPartitionReport;
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

    <VALUE> Map<TopicPartitionReport, List<KeyValueReport<VALUE>>> toKeyValueReportsPerTopicPartition(
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

    <VALUE> List<KeyValueReport<VALUE>> toKeyValueReports(Collection<ConsumerRecord<String, VALUE>> records) {
        return records
                .stream()
                .map(this::toKeyValueReport)
                .collect(Collectors.toList());
    }

    <VALUE> KeyValueReport<VALUE> toKeyValueReport(ConsumerRecord<String, VALUE> record) {
        return new KeyValueReport<>(record.key(), record.value());
    }

    TopicPartitionReport toTopicPartition(ConsumerRecord<?, ?> record) {
        return new TopicPartitionReport(record.topic(), record.partition());
    }

    ExceptionReport toExceptionReport(Exception exception) {
        if (exception instanceof ListenerExecutionFailedException) {
            Exception cause = (Exception) exception.getCause();
            return new ExceptionReport(cause.getClass(), cause.getMessage());
        }
        return new ExceptionReport(
                exception.getClass(),
                exception.getMessage()
        );
    }

    Map<TopicPartitionReport, Long> toTopicPartitionAssignments(Map<TopicPartition, Long> assignments) {
        return assignments
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        entry -> toTopicPartitionReport(entry.getKey()),
                        Map.Entry::getValue
                ));
    }

    Set<TopicPartitionReport> toTopicPartitionReports(Collection<TopicPartition> partitions) {
        return partitions
                .stream()
                .map(this::toTopicPartitionReport)
                .collect(Collectors.toSet());
    }

    TopicPartitionReport toTopicPartitionReport(TopicPartition partition) {
        return new TopicPartitionReport(partition.topic(), partition.partition());
    }

}
