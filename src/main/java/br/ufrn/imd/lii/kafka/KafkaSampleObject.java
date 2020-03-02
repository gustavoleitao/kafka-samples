package br.ufrn.imd.lii.kafka;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class KafkaSampleObject {

    private String type;

    private BigDecimal amount;

}
