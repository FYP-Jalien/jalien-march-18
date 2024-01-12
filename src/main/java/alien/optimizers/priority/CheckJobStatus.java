package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.optimizers.Optimizer;

import java.util.logging.Logger;

//TODO rename
public class CheckSubjobStatus extends Optimizer {
    static final Logger logger = ConfigUtils.getLogger(PriorityReconciliationService.class.getCanonicalName());

    static final Monitor monitor = MonitorFactory.getMonitor(PriorityReconciliationService.class.getCanonicalName());

    
}
