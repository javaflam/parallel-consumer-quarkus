package org.acme;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

import org.jboss.logging.Logger;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class AppLifecycleBean {
    
    private static final Logger LOG = Logger.getLogger("AppLifecycleBean");

    void onStart(@Observes StartupEvent ev) {               
        LOG.info("The application is starting...");
    }

    void onStop(@Observes ShutdownEvent ev) {               
        LOG.info("The application is stopping...");
    }
}
