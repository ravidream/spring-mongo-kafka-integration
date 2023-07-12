package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.model.Employee;
import com.example.repo.EmployeeRepo;
import com.example.service.SequenceGeneratorService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class KafkaMessageConsumer {
	@Autowired
	private EmployeeRepo employeeRepo;
	
	@Autowired
	private SequenceGeneratorService sequenceGeneratorService;
	
	@KafkaListener(topics = "test", groupId = "test-group")
	public void listener(ConsumerRecord<String, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
	    System.out.println("Received Message value : " + consumerRecord.value());
	    ObjectMapper objectMapper = new ObjectMapper();
        Employee employee = objectMapper.readValue(consumerRecord.value().toString(), Employee.class);
	    employee.setId(sequenceGeneratorService.generateSequence(Employee.SEQUENCE_NAME));
		employeeRepo.save(employee);
	}
}

