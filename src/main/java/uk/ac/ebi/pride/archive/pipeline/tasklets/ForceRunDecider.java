package uk.ac.ebi.pride.archive.pipeline.tasklets;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@StepScope
@Component
public class ForceRunDecider extends AbstractTasklet {

    // possible decider outcomes
    public static final String YES = "YES";
    public static final String NO = "NO";

    @Value("#{jobParameters['forceToRun']?:false}")
    private boolean forceToRun;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        logger.info("Force to run? {}", forceToRun);
        setExitStatus(chunkContext, forceToRun ? YES : NO);
        return RepeatStatus.FINISHED;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
    }

    public void setForceToRun(Boolean forceToRun) {
        if (forceToRun != null) {
            this.forceToRun = forceToRun;
        }
    }
}
