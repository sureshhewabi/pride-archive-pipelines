package uk.ac.ebi.pride.archive.pipeline.tasklets;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.Map;

public abstract class AbstractTasklet implements Tasklet, InitializingBean {
    /**
     * Update step execution context using a given map of values
     *
     * @param chunkContext tasklet chunk context
     * @param values       a map of values
     */
    protected void updateStepExecutionContext(ChunkContext chunkContext, Map<String, Object> values) {
        Assert.notNull(values, "Cannot add null values to chunkContext");

        StepContext stepContext = chunkContext.getStepContext();
        ExecutionContext stepExecutionContext = stepContext.getStepExecution().getExecutionContext();

        for (Map.Entry<String, Object> entry : values.entrySet()) {
            stepExecutionContext.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Update step execution context using a pair of key and value
     *
     * @param chunkContext tasklet chunk context
     * @param key          key to identify the value
     * @param value        value assigned to the given key
     */
    protected void updateStepExecutionContext(ChunkContext chunkContext, String key, Object value) {
        Assert.notNull(key, "Context key cannot be null");
        Assert.notNull(value, "Context value cannot be null");

        StepContext stepContext = chunkContext.getStepContext();
        ExecutionContext stepExecutionContext = stepContext.getStepExecution().getExecutionContext();

        stepExecutionContext.put(key, value);
    }

    /**
     * Sets the ExitStatus value for the step executing this tasklet
     *
     * @param chunkContext tasklet chunk context
     * @param exitStatus   the exit status to set
     */
    protected void setExitStatus(ChunkContext chunkContext, String exitStatus) {
        Assert.notNull(exitStatus, "Exit status cannot be null");
        StepContext stepContext = chunkContext.getStepContext();

        stepContext.getStepExecution().setExitStatus(new ExitStatus(exitStatus));

    }

}
