package com.jpmc.midascore.controller;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.web.bind.annotation.*;

@RestController
public class BalanceController {

    private final UserRepository userRepository;

    public BalanceController(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam long userId) {

        UserRecord user = userRepository.findById(userId).orElse(null);

        if (user == null) {
            // user does not exist → balance is 0
            return new Balance(0);
        }

        // return actual balance
        return new Balance(user.getBalance());
    }
}
