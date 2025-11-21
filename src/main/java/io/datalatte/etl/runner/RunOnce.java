package io.datalatte.etl.runner;

import io.datalatte.etl.pipeline.Pipeline;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RunOnce implements CommandLineRunner {

    private final Pipeline pipeline;

    @Override
    public void run(String... args) {
        System.out.println("RUNONCE_START");
        pipeline.runOnce();
    }
}
