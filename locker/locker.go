package locker

import (
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
)

type LockedRunner struct {
	HeartbeatInterval time.Duration
	BBS               bbs.ConvergerBBS
	Logger            lager.Logger
	Runner            ifrit.Runner
}

func (runner *LockedRunner) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	uuid, err := uuid.NewV4()
	if err != nil {
		return err
	}

	statusChannel, releaseLock, err := runner.BBS.MaintainConvergeLock(runner.HeartbeatInterval, uuid.String())
	if err != nil {
		runner.Logger.Error("failed-acquiring-converge-lock", err)
		return err
	}

	close(ready)

	once := &sync.Once{}

	var process ifrit.Process
	var processExited <-chan error

	var gotLock time.Time

	for {
		select {
		case err := <-processExited:
			return err

		case sig := <-signals:
			if process != nil {
				process.Signal(sig)
			}

			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				once.Do(func() {
					// should trigger status channel closing once released
					close(releaseLock)
				})
			}

		case locked, ok := <-statusChannel:
			if !ok {
				if processExited == nil {
					return nil
				}

				statusChannel = nil
				break
			}

			if locked {
				if gotLock.IsZero() {
					gotLock = time.Now()
				}

				if process == nil {
					process = ifrit.Envoke(runner.Runner)
					processExited = process.Wait()
				}
			} else if process != nil {
				runner.Logger.Error("lost-lock", nil, lager.Data{
					"had-lock-for": time.Since(gotLock).String(),
				})

				process.Signal(syscall.SIGINT)

				gotLock = time.Time{}
			}
		}
	}

	return nil
}
