package hystrix

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type runFunc func() error
type fallbackFunc func(error) error
type runFuncC func(context.Context) error
type fallbackFuncC func(context.Context, error) error

// A CircuitError is an error which models various failure states of execution,
// such as the circuit being open or a timeout.
type CircuitError struct {
	Message string
}

func (e CircuitError) Error() string {
	return "hystrix: " + e.Message
}

// command models the state used for a single execution on a circuit. "hystrix command" is commonly
// used to describe the pairing of your run/fallback functions with a circuit.
type command struct {
	sync.Mutex

	ticket      *struct{}       // 保存获取到的令牌
	start       time.Time       // 调用开始时间
	errChan     chan error      // 错误信息 channel
	finished    chan bool       // 完成信息 channel
	circuit     *CircuitBreaker // 熔断器
	run         runFuncC        // 封装正常运行的方法
	fallback    fallbackFuncC   // 封装降级方法
	runDuration time.Duration   // 运行时间
	events      []string        // 保存档期调用的所有事件，方便最终统一上报
}

var (
	// ErrMaxConcurrency occurs when too many of the same named command are executed at the same time.
	ErrMaxConcurrency = CircuitError{Message: "max concurrency"}
	// ErrCircuitOpen returns when an execution attempt "short circuits". This happens due to the circuit being measured as unhealthy.
	ErrCircuitOpen = CircuitError{Message: "circuit open"}
	// ErrTimeout occurs when the provided function takes too long to execute.
	ErrTimeout = CircuitError{Message: "timeout"}
)

// Go runs your function while tracking the health of previous calls to it.
// If your function begins slowing down or failing repeatedly, we will block
// new calls to it for you to give the dependent service time to repair.
//
// Define a fallback function if you want to define some code to execute during outages.
// Go 以异步非阻塞方式调用 run 函数，返回一个chan error，可以监听调用结果
func Go(name string, run runFunc, fallback fallbackFunc) chan error {
	runC := func(ctx context.Context) error {
		return run()
	}
	var fallbackC fallbackFuncC
	if fallback != nil {
		fallbackC = func(ctx context.Context, err error) error {
			return fallback(err)
		}
	}
	return GoC(context.Background(), name, runC, fallbackC)
}

// GoC runs your function while tracking the health of previous calls to it.
// If your function begins slowing down or failing repeatedly, we will block
// new calls to it for you to give the dependent service time to repair.
//
// Define a fallback function if you want to define some code to execute during outages.
//
// GoC 通过跟踪先前的调用健康状态来运行 run 函数
// 如果你的函数开始变慢或反复失败，我们将阻止对它的新调用，以便让依赖服务有时间修复。
// 如果想在熔断期间执行降级操作，可以定义 fallback 函数。
// 返回值为 chan error 类型，需要调用端自行接收处理
func GoC(ctx context.Context, name string, run runFuncC, fallback fallbackFuncC) chan error {
	// 对于每次调用新建 command 操作命令
	cmd := &command{
		run:      run,
		fallback: fallback,
		start:    time.Now(),
		errChan:  make(chan error, 1),
		finished: make(chan bool, 1),
	}

	// dont have methods with explicit params and returns
	// let data come in and out naturally, like with any closure
	// explicit error return to give place for us to kill switch the operation (fallback)

	// 通过 command name 参数获取熔断器，当获取失败时返回错误
	circuit, _, err := GetCircuit(name)
	if err != nil {
		cmd.errChan <- err
		return cmd.errChan
	}
	cmd.circuit = circuit
	ticketCond := sync.NewCond(cmd)
	// 标示是否执行了获取令牌操作，不管获取失败与否
	ticketChecked := false
	// When the caller extracts error from returned errChan, it's assumed that
	// the ticket's been returned to executorPool. Therefore, returnTicket() can
	// not run after cmd.errorWithFallback().
	//
	// 当调用者从返回的 errChan 中获取到错误，那么假定 ticket 已返回给 executorPool。
	// 因此，在 cmd.errorWithFallback() 之后不能运行 returnTicket()。
	//
	// returnTicket 归还令牌函数
	returnTicket := func() {
		cmd.Lock()
		// Avoid releasing before a ticket is acquired.
		// 阻塞等待，避免在获取到令牌之前就执行归还操作
		for !ticketChecked {
			ticketCond.Wait()
		}
		// 归还令牌
		cmd.circuit.executorPool.Return(cmd.ticket)
		cmd.Unlock()
	}
	// Shared by the following two goroutines. It ensures only the faster
	// goroutine runs errWithFallback() and reportAllEvent().
	// returnOnce 用于确保只有更快的 goroutine 运行 errWithFallback() 和 reportAllEvent()
	returnOnce := &sync.Once{}
	// 上报当前 command 所有事件
	reportAllEvent := func() {
		err := cmd.circuit.ReportEvent(cmd.events, cmd.start, cmd.runDuration)
		if err != nil {
			log.Printf(err.Error())
		}
	}

	// 创建协程用于检查熔断器状态、获取令牌、上报事件、执行用户函数
	go func() {
		// 执行成功后，写入 finished 信号，通知主 goroutine 结束
		defer func() { cmd.finished <- true }()

		// Circuits get opened when recent executions have shown to have a high error rate.
		// Rejecting new executions allows backends to recover, and the circuit will allow
		// new traffic when it feels a healthly state has returned.
		//
		// 当最近的执行出现高错误率时，熔断器会打开。拒绝新的执行允许后端恢复，熔断器会在觉得健康状态恢复时允许新流量进入。
		//
		// 当熔断器不允许新的请求时（此时熔断器已经打开）
		if !cmd.circuit.AllowRequest() {
			cmd.Lock()
			// It's safe for another goroutine to go ahead releasing a nil ticket.
			// 当另一个 goroutine 提前执行释放令牌时，是安全的。
			ticketChecked = true

			// 唤醒一个正在等待的协程
			ticketCond.Signal()
			cmd.Unlock()
			returnOnce.Do(func() {
				returnTicket()                             // 归还令牌
				cmd.errorWithFallback(ctx, ErrCircuitOpen) // 上报当前错误，并尝试执行降级操作
				reportAllEvent()                           // 上报当前 command 所有事件
			})
			return
		}

		// As backends falter, requests take longer but don't always fail.
		//
		// When requests slow down but the incoming rate of requests stays the same, you have to
		// run more at a time to keep up. By controlling concurrency during these situations, you can
		// shed load which accumulates due to the increasing ratio of active commands to incoming requests.
		//
		// 加锁获取令牌
		cmd.Lock()
		select {
		case cmd.ticket = <-circuit.executorPool.Tickets: // 获取令牌成功
			ticketChecked = true
			ticketCond.Signal()
			cmd.Unlock()
		default: // 获取令牌失败（当超过 command 允许的最大并发数时）
			ticketChecked = true
			ticketCond.Signal()
			cmd.Unlock()
			returnOnce.Do(func() {
				returnTicket()
				cmd.errorWithFallback(ctx, ErrMaxConcurrency)
				reportAllEvent()
			})
			return
		}

		runStart := time.Now()

		// 真正执行用户的正常的 run 函数
		runErr := run(ctx)
		returnOnce.Do(func() {
			defer reportAllEvent()                 // 上报当前 command 所有事件
			cmd.runDuration = time.Since(runStart) // 记录运行时间
			returnTicket()                         // 归还令牌
			if runErr != nil {                     // 检查错误
				cmd.errorWithFallback(ctx, runErr) // 保存错误事件，并尝试执行 fallback 函数
				return
			}
			cmd.reportEvent("success") // 记录成功事件
		})
	}()

	// 创建协程用于检查当前操作是否超时
	go func() {
		// 根据全局超时事件创建定时器
		timer := time.NewTimer(getSettings(name).Timeout)
		defer timer.Stop()

		select {
		case <-cmd.finished: //检查是否己经完成
			// returnOnce has been executed in another goroutine
			// returnOnce 已经在另外的协程处理，这里不需要再处理
		case <-ctx.Done(): // 检查 context 自定义超时
			returnOnce.Do(func() {
				returnTicket()
				cmd.errorWithFallback(ctx, ctx.Err())
				reportAllEvent()
			})
			return
		case <-timer.C: // 检查 command 初始化定义的超时
			returnOnce.Do(func() {
				returnTicket()
				cmd.errorWithFallback(ctx, ErrTimeout)
				reportAllEvent()
			})
			return
		}
	}()

	return cmd.errChan
}

// Do runs your function in a synchronous manner, blocking until either your function succeeds
// or an error is returned, including hystrix circuit errors
// Do 以同步阻塞的方式调用runFunc，直到runFunc成功或返回错误，包括触发器熔断的错误
func Do(name string, run runFunc, fallback fallbackFunc) error {
	// 封装正常函数的调用
	runC := func(ctx context.Context) error {
		return run()
	}

	// 定义了降级函数，进行封装
	var fallbackC fallbackFuncC
	if fallback != nil {
		fallbackC = func(ctx context.Context, err error) error {
			return fallback(err)
		}
	}

	return DoC(context.Background(), name, runC, fallbackC)
}

// DoC runs your function in a synchronous manner, blocking until either your function succeeds
// or an error is returned, including hystrix circuit errors
// Doc 以同步阻塞的方式调用runFuncC，直到runFuncC成功或返回错误，包括触发器熔断的错误
func DoC(ctx context.Context, name string, run runFuncC, fallback fallbackFuncC) error {
	// 初始化 done chan 用于接收完成信息，当失败时不会往 done chan 写入数据
	done := make(chan struct{}, 1)

	// 对正常的 run 函数进行二次封装，只有当内部执行成功时才会向 done chan 写入数据
	r := func(ctx context.Context) error {
		err := run(ctx)
		if err != nil {
			return err
		}

		done <- struct{}{}
		return nil
	}

	// 对降级函数进行二次封装，只有当内部执行成功时才会向 done chan 写入数据
	f := func(ctx context.Context, e error) error {
		err := fallback(ctx, e)
		if err != nil {
			return err
		}

		done <- struct{}{}
		return nil
	}

	// 最后都是统一调用 GoC 函数
	var errChan chan error
	if fallback == nil {
		errChan = GoC(ctx, name, r, nil)
	} else {
		errChan = GoC(ctx, name, r, f)
	}

	// select 阻塞等到接收 done 信号，或者 errChan 接收到错误信息
	select {
	case <-done:
		return nil
	case err := <-errChan:
		return err
	}
}

// reportEvent 产生的时间先保存到 events 数组中，最后统一调用reportAllEvent上报
func (c *command) reportEvent(eventType string) {
	c.Lock()
	defer c.Unlock()

	c.events = append(c.events, eventType)
}

// errorWithFallback triggers the fallback while reporting the appropriate metric events.
// errorWithFallback 保存相关事件和执行 fallback 函数
func (c *command) errorWithFallback(ctx context.Context, err error) {
	eventType := "failure"
	if err == ErrCircuitOpen {
		eventType = "short-circuit"
	} else if err == ErrMaxConcurrency {
		eventType = "rejected"
	} else if err == ErrTimeout {
		eventType = "timeout"
	} else if err == context.Canceled {
		eventType = "context_canceled"
	} else if err == context.DeadlineExceeded {
		eventType = "context_deadline_exceeded"
	}

	c.reportEvent(eventType)
	fallbackErr := c.tryFallback(ctx, err)
	if fallbackErr != nil {
		c.errChan <- fallbackErr
	}
}

// tryFallback 尝试执行 fallback 函数，并记录相关事件
func (c *command) tryFallback(ctx context.Context, err error) error {
	if c.fallback == nil {
		// If we don't have a fallback return the original error.
		return err
	}

	fallbackErr := c.fallback(ctx, err)
	if fallbackErr != nil {
		c.reportEvent("fallback-failure")
		return fmt.Errorf("fallback failed with '%v'. run error was '%v'", fallbackErr, err)
	}

	c.reportEvent("fallback-success")

	return nil
}
