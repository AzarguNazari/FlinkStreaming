package event;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BankAccount {
    private int id;
    private Long accountNumber;
    private String BSZ;
    private Double balance;
    private String holderName;
    private long timestamp;
}
