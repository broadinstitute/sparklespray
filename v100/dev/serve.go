package dev

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"github.com/urfave/cli"
)

// serveShutdownGrace is how long in-flight HTTP requests get to finish once
// shutdown starts.
const serveShutdownGrace = 10 * time.Second

// ServeCommand returns the top-level "serve" command, which runs the whole
// control plane — the monitor and the dashboard-backend (REST API + embedded
// UI) — in a single process. It lives in this package because it reuses the
// dashboard-backend and monitor wiring here, and is registered as a top-level
// command by cmd/sparkles/main.go.
func ServeCommand() cli.Command {
	return cli.Command{
		Name:  "serve",
		Usage: "Run the control plane: the monitor and the dashboard-backend (REST API + UI) in one process",
		Flags: []cli.Flag{
			cli.StringFlag{Name: "project", Usage: "GCP project ID (required)"},
			cli.StringFlag{Name: "db", Value: defaultDB, Usage: "Firestore database"},
			cli.StringFlag{Name: "addr", Value: ":8080", Usage: "address for the dashboard-backend to listen on"},
			cli.StringFlag{Name: "prefix", Usage: "URL path prefix under which all routes are served, e.g. \"sparkles\" serves everything under /sparkles/... (default: none, serve at the root)"},
			cli.BoolFlag{Name: "verbose, v", Usage: "log a message at the start of every monitor poll"},
		},
		Action: runServe,
	}
}

func runServe(c *cli.Context) error {
	project := c.String("project")
	if project == "" {
		return fmt.Errorf("--project is required")
	}
	db := c.String("db")
	addr := c.String("addr")
	prefix := normalizePrefix(c.String("prefix"))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("Connecting to project %s, database %s", project, db)
	fsClient, err := firestore.NewClientWithDatabase(ctx, project, db)
	if err != nil {
		return fmt.Errorf("creating firestore client: %w", err)
	}
	defer fsClient.Close()

	psClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating pubsub client: %w", err)
	}
	defer psClient.Close()

	// No --linger here: unlike "dev monitor", a server shouldn't shut itself
	// down when the queue goes quiet.
	m, stopMonitor, err := newMonitor(ctx, project, db, fsClient, psClient, c.Bool("verbose"), 0)
	if err != nil {
		return err
	}
	defer stopMonitor()

	handler, err := newDashboardHandler(ctx, project, fsClient, psClient, prefix)
	if err != nil {
		return err
	}
	httpSrv := &http.Server{Addr: addr, Handler: handler}

	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		m.RunMonitorLoop(ctx)
	}()

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("serve: dashboard-backend listening on %s", addr)
		err := httpSrv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serverErr <- err
	}()

	// Any one of these ending takes the whole process down: a signal, the
	// listener failing, or the monitor loop exiting (which it only does on a
	// fatal Pub/Sub error, since linger is off).
	var runErr error
	select {
	case <-ctx.Done():
		log.Printf("serve: shutting down")
	case runErr = <-serverErr:
		if runErr != nil {
			runErr = fmt.Errorf("dashboard-backend: %w", runErr)
		}
	case <-monitorDone:
		log.Printf("serve: monitor loop exited; shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), serveShutdownGrace)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil && runErr == nil {
		runErr = fmt.Errorf("shutting down dashboard-backend: %w", err)
	}

	// Cancel ctx (if a signal didn't already) so the monitor loop returns, and
	// wait for it before the deferred client Close calls run.
	stop()
	<-monitorDone

	return runErr
}
