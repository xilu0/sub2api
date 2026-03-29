package service

import (
	"context"
	"sort"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

const groupSchedulerQueueSize = 512

// groupScheduleTask 封装一次串行调度任务的全部输入。
// 窗口费用（WindowCost）和 RPM 预取数据通过 ctx 传递，避免串行 goroutine 内重复 Redis 往返。
// 并发计数（Concurrency）在串行 goroutine 内实时读取，保证精确性。
type groupScheduleTask struct {
	ctx              context.Context
	group            *Group
	groupID          *int64
	sessionHash      string
	requestedModel   string
	excludedIDs      map[int64]struct{}
	stickyAccountID  int64
	accounts         []Account // listSchedulableAccounts 过滤后的账号列表
	platform         string
	hasForcePlatform bool
	useMixed         bool
	preferOAuth      bool
	cfg              config.GatewaySchedulingConfig
	result           chan groupScheduleResult
}

type groupScheduleResult struct {
	res *AccountSelectionResult
	err error
}

// groupScheduler 为单个分组维护一个串行调度 goroutine，彻底消除 TOCTOU 竞争。
//
// 问题背景：
//
//	并发请求同时读取 Redis 负载快照（GetAccountsLoadBatch），
//	都看到账号 A 有剩余槽位，于是全部尝试抢槽，仅有少数成功。
//	失败者落入下一优先级账号，导致流量在多个账号间分散（scatter），
//	高优先级账号未打满便向低优先级溢出。
//
// 解决方案：
//
//	每个分组一个串行 goroutine，依次处理选号任务。
//	任务内 GetAccountsLoadBatch → sort → AcquireAccountSlot 是原子序列：
//	  - 后续请求读到的负载已反映前一请求的槽位变化，无陈旧快照；
//	  - 高优先级账号被真正打满后，下一个请求才自然溢出到次优先级。
//
// 性能：
//
//	序列化仅限于"读负载 + 拿槽"决策（典型 <2ms），HTTP 转发并发度不受影响。
//	分组间完全并行，groupScheduler 仅阻塞同一分组的并发请求。
type groupScheduler struct {
	queue   chan *groupScheduleTask
	service *GatewayService
}

func newGroupScheduler(s *GatewayService) *groupScheduler {
	gs := &groupScheduler{
		queue:   make(chan *groupScheduleTask, groupSchedulerQueueSize),
		service: s,
	}
	go gs.run()
	return gs
}

// run 是调度 goroutine，串行处理队列中的所有任务。
func (gs *groupScheduler) run() {
	for task := range gs.queue {
		res, err := gs.service.selectAccountSerial(task)
		task.result <- groupScheduleResult{res: res, err: err}
	}
}

// schedule 将任务入队并阻塞等待结果。
// 若调用方 ctx 已超时或取消，立即返回 ctx.Err()，不阻塞调度队列。
func (gs *groupScheduler) schedule(task *groupScheduleTask) (*AccountSelectionResult, error) {
	select {
	case gs.queue <- task:
	case <-task.ctx.Done():
		return nil, task.ctx.Err()
	}
	select {
	case result := <-task.result:
		return result.res, result.err
	case <-task.ctx.Done():
		return nil, task.ctx.Err()
	}
}

// getOrCreateGroupScheduler 获取或懒创建指定分组的串行调度器（per-group 粒度）。
// 同组所有模型共用一个 goroutine，消除跨模型的账号池 TOCTOU 竞争。
// 支持未经 NewGatewayService 初始化的测试用例（直接构造 struct）。
func (s *GatewayService) getOrCreateGroupScheduler(key string) *groupScheduler {
	s.groupSchedulersMu.Lock()
	defer s.groupSchedulersMu.Unlock()
	if s.groupSchedulers == nil {
		s.groupSchedulers = make(map[string]*groupScheduler)
	}
	if gs, ok := s.groupSchedulers[key]; ok {
		return gs
	}
	gs := newGroupScheduler(s)
	s.groupSchedulers[key] = gs
	return gs
}

// selectAccountSerial 在调度 goroutine 内串行执行账号选择，保证同一分组的选号操作不并发。
//
// 调度分层：
//
//	Layer 1  : 模型路由（anthropic 平台分组有路由规则时，限定候选账号集合）
//	Layer 1.5: 粘性会话（优先复用 KV 缓存；不做主动抢占以最大化缓存命中）
//	             a. 可调度 + 有槽 → 直接复用（缓存命中）
//	             b. 可调度 + 槽满 → WaitPlan 等该账号（保留缓存，不跳号散流）
//	             c. 不可调度或等待队列满 → 清除 sticky，落入 Layer 2
//	Layer 2  : 按 priority ASC 顺序拿槽（串行消除 TOCTOU，精确打满高优先级账号）
//	Layer 3  : 兜底 WaitPlan（选 priority 最小的可调度账号排队等待）
func (s *GatewayService) selectAccountSerial(task *groupScheduleTask) (*AccountSelectionResult, error) {
	ctx := task.ctx
	cfg := task.cfg
	groupID := task.groupID
	sessionHash := task.sessionHash
	requestedModel := task.requestedModel
	stickyAccountID := task.stickyAccountID
	preferOAuth := task.preferOAuth

	isExcluded := func(accountID int64) bool {
		if task.excludedIDs == nil {
			return false
		}
		_, excluded := task.excludedIDs[accountID]
		return excluded
	}

	accountByID := make(map[int64]*Account, len(task.accounts))
	for i := range task.accounts {
		accountByID[task.accounts[i].ID] = &task.accounts[i]
	}

	// ============ Layer 1: 模型路由 ============
	var routingAccountIDs []int64
	if task.group != nil && requestedModel != "" && task.group.Platform == PlatformAnthropic {
		routingAccountIDs = task.group.GetRoutingAccountIDs(requestedModel)
	}
	if len(routingAccountIDs) > 0 {
		result, err, handled := s.selectAccountSerialRouted(ctx, task, accountByID, routingAccountIDs, isExcluded)
		if handled {
			return result, err
		}
		// 路由账号全不可用，回退到 Layer 2
	}

	// 构建 Layer 2/3 候选（过滤平台、模型、配额、窗口费用、RPM）
	candidates := s.buildLoadAwareCandidates(ctx, task.accounts, task.excludedIDs, task.platform, task.useMixed, requestedModel)

	// ============ Layer 1.5: 粘性会话 ============
	// 目标：复用 KV 缓存，不做主动抢占（同事要求保留缓存）。
	// 串行 goroutine 保证此处读写无竞争，无需 SetSessionAccountIDIfBetter CAS。
	if len(routingAccountIDs) == 0 && sessionHash != "" && stickyAccountID > 0 && !isExcluded(stickyAccountID) {
		account, ok := accountByID[stickyAccountID]
		if !ok {
			// 账号已从可用列表移除（被删除/禁用），清除绑定
			if s.cache != nil {
				_ = s.cache.DeleteSessionAccountID(ctx, derefGroupID(groupID), sessionHash)
			}
		} else if shouldClearStickySession(account, requestedModel) {
			// 账号状态异常或模型限速，清除绑定
			if s.cache != nil {
				_ = s.cache.DeleteSessionAccountID(ctx, derefGroupID(groupID), sessionHash)
			}
		} else if s.isAccountInGroup(account, groupID) &&
			s.isAccountAllowedForPlatform(account, task.platform, task.useMixed) &&
			(requestedModel == "" || s.isModelSupportedByAccountWithContext(ctx, account, requestedModel)) &&
			s.isAccountSchedulableForModelSelection(ctx, account, requestedModel) &&
			s.isAccountSchedulableForQuota(account) &&
			s.isAccountSchedulableForWindowCost(ctx, account, true) &&
			s.isAccountSchedulableForRPM(ctx, account, true) {
			// 账号可调度：尝试拿槽
			slotResult, slotErr := s.tryAcquireAccountSlot(ctx, stickyAccountID, account.Concurrency)
			if slotErr == nil && slotResult.Acquired {
				if s.checkAndRegisterSession(ctx, account, sessionHash) {
					// Layer 1.5 命中：缓存复用
					return &AccountSelectionResult{
						Account:     account,
						Acquired:    true,
						ReleaseFunc: slotResult.ReleaseFunc,
					}, nil
				}
				slotResult.ReleaseFunc()
				// 会话限制满，落入 Layer 2
			} else {
				// 槽位已满：等待该账号释放，保留缓存（不跳号）
				if s.concurrencyService != nil {
					waitingCount, _ := s.concurrencyService.GetAccountWaitingCount(ctx, stickyAccountID)
					if waitingCount < cfg.StickySessionMaxWaiting {
						if s.checkAndRegisterSession(ctx, account, sessionHash) {
							return &AccountSelectionResult{
								Account: account,
								WaitPlan: &AccountWaitPlan{
									AccountID:      stickyAccountID,
									MaxConcurrency: account.Concurrency,
									Timeout:        cfg.StickySessionWaitTimeout,
									MaxWaiting:     cfg.StickySessionMaxWaiting,
								},
							}, nil
						}
					}
				}
				// 等待队列也满：清除 sticky，让 Layer 2 重新选号
				if s.cache != nil {
					_ = s.cache.DeleteSessionAccountID(ctx, derefGroupID(groupID), sessionHash)
				}
			}
		} else {
			// 账号不可调度，清除绑定
			if s.cache != nil {
				_ = s.cache.DeleteSessionAccountID(ctx, derefGroupID(groupID), sessionHash)
			}
		}
	}

	if len(candidates) == 0 {
		return nil, ErrNoAvailableAccounts
	}

	// ============ Layer 2: 按 priority ASC 串行拿槽 ============
	// 串行执行保证：GetAccountsLoadBatch 返回的是最新负载（已含上一个请求的拿槽写入），
	// 不存在并发快照陈旧问题，高优先级账号真正打满后下一个请求才溢出。
	accountLoads := make([]AccountWithConcurrency, 0, len(candidates))
	for _, acc := range candidates {
		accountLoads = append(accountLoads, AccountWithConcurrency{
			ID:             acc.ID,
			MaxConcurrency: acc.EffectiveLoadFactor(),
		})
	}
	loadMap, loadErr := s.concurrencyService.GetAccountsLoadBatch(ctx, accountLoads)

	if loadErr == nil {
		var available []accountWithLoad
		for _, acc := range candidates {
			loadInfo := loadMap[acc.ID]
			if loadInfo == nil {
				loadInfo = &AccountLoadInfo{AccountID: acc.ID}
			}
			if loadInfo.LoadRate < 100 {
				available = append(available, accountWithLoad{account: acc, loadInfo: loadInfo})
			}
		}

		// 按 priority ASC → loadRate ASC → OAuth优先（Gemini） → LRU 排序
		sort.SliceStable(available, func(i, j int) bool {
			a, b := available[i], available[j]
			if a.account.Priority != b.account.Priority {
				return a.account.Priority < b.account.Priority
			}
			if a.loadInfo.LoadRate != b.loadInfo.LoadRate {
				return a.loadInfo.LoadRate < b.loadInfo.LoadRate
			}
			// OAuth 优先（Gemini 平台）
			if preferOAuth {
				aIsOAuth := a.account.Type == AccountTypeOAuth
				bIsOAuth := b.account.Type == AccountTypeOAuth
				if aIsOAuth != bIsOAuth {
					return aIsOAuth
				}
			}
			// LRU：最久未用优先（nil 视为最久未用）
			switch {
			case a.account.LastUsedAt == nil && b.account.LastUsedAt != nil:
				return true
			case a.account.LastUsedAt != nil && b.account.LastUsedAt == nil:
				return false
			case a.account.LastUsedAt != nil && b.account.LastUsedAt != nil:
				return a.account.LastUsedAt.Before(*b.account.LastUsedAt)
			default:
				return false
			}
		})

		for _, item := range available {
			slotResult, slotErr := s.tryAcquireAccountSlot(ctx, item.account.ID, item.account.Concurrency)
			if slotErr == nil && slotResult.Acquired {
				if !s.checkAndRegisterSession(ctx, item.account, sessionHash) {
					slotResult.ReleaseFunc()
					continue
				}
				// 新会话：写入 sticky 绑定（串行无竞争，直接 Set）
				if sessionHash != "" && s.cache != nil {
					_ = s.cache.SetSessionAccountID(ctx, derefGroupID(groupID), sessionHash, item.account.ID, stickySessionTTL)
				}
				return &AccountSelectionResult{
					Account:     item.account,
					Acquired:    true,
					ReleaseFunc: slotResult.ReleaseFunc,
				}, nil
			}
		}
	} else {
		// Redis 获取负载失败，回退到传统顺序选择
		if result, ok := s.tryAcquireByLegacyOrder(ctx, candidates, groupID, sessionHash, preferOAuth); ok {
			return result, nil
		}
	}

	// ============ Layer 3: 兜底 WaitPlan ============
	// 所有候选账号槽位已满，选 priority 最小的账号排队等待。
	s.sortCandidatesForFallback(candidates, preferOAuth, cfg.FallbackSelectionMode)
	for _, acc := range candidates {
		if !s.checkAndRegisterSession(ctx, acc, sessionHash) {
			continue
		}
		return &AccountSelectionResult{
			Account: acc,
			WaitPlan: &AccountWaitPlan{
				AccountID:      acc.ID,
				MaxConcurrency: acc.Concurrency,
				Timeout:        cfg.FallbackWaitTimeout,
				MaxWaiting:     cfg.FallbackMaxWaiting,
			},
		}, nil
	}
	return nil, ErrNoAvailableAccounts
}

// selectAccountSerialRouted 处理 Layer 1 模型路由逻辑（串行版本）。
//
// 返回 (result, err, handled)：
//   - handled=true：已选出账号或确认无可用，调用方直接返回
//   - handled=false：路由账号全不可用，调用方继续 Layer 2
func (s *GatewayService) selectAccountSerialRouted(
	ctx context.Context,
	task *groupScheduleTask,
	accountByID map[int64]*Account,
	routingAccountIDs []int64,
	isExcluded func(int64) bool,
) (*AccountSelectionResult, error, bool) {
	cfg := task.cfg
	groupID := task.groupID
	sessionHash := task.sessionHash
	requestedModel := task.requestedModel
	stickyAccountID := task.stickyAccountID

	// 构建路由候选列表
	var routingCandidates []*Account
	for _, id := range routingAccountIDs {
		if isExcluded(id) {
			continue
		}
		acc, ok := accountByID[id]
		if !ok || !s.isAccountSchedulableForSelection(acc) {
			continue
		}
		if !s.isAccountAllowedForPlatform(acc, task.platform, task.useMixed) {
			continue
		}
		if requestedModel != "" && !s.isModelSupportedByAccountWithContext(ctx, acc, requestedModel) {
			continue
		}
		if !s.isAccountSchedulableForModelSelection(ctx, acc, requestedModel) {
			continue
		}
		if !s.isAccountSchedulableForQuota(acc) {
			continue
		}
		if !s.isAccountSchedulableForWindowCost(ctx, acc, false) {
			continue
		}
		if !s.isAccountSchedulableForRPM(ctx, acc, false) {
			continue
		}
		routingCandidates = append(routingCandidates, acc)
	}

	if len(routingCandidates) == 0 {
		return nil, nil, false
	}

	// Layer 1.5（路由范围内）：粘性会话
	if sessionHash != "" && stickyAccountID > 0 &&
		containsInt64(routingAccountIDs, stickyAccountID) && !isExcluded(stickyAccountID) {
		stickyAcc, ok := accountByID[stickyAccountID]
		if !ok {
			// 粘性账号在路由列表中但不在可用账号里（被删除/禁用），清除绑定
			if s.cache != nil {
				_ = s.cache.DeleteSessionAccountID(ctx, derefGroupID(groupID), sessionHash)
			}
		} else if s.isAccountSchedulableForSelection(stickyAcc) &&
				s.isAccountAllowedForPlatform(stickyAcc, task.platform, task.useMixed) &&
				(requestedModel == "" || s.isModelSupportedByAccountWithContext(ctx, stickyAcc, requestedModel)) &&
				s.isAccountSchedulableForModelSelection(ctx, stickyAcc, requestedModel) &&
				s.isAccountSchedulableForQuota(stickyAcc) &&
				s.isAccountSchedulableForWindowCost(ctx, stickyAcc, true) &&
				s.isAccountSchedulableForRPM(ctx, stickyAcc, true) {
				slotResult, slotErr := s.tryAcquireAccountSlot(ctx, stickyAccountID, stickyAcc.Concurrency)
				if slotErr == nil && slotResult.Acquired {
					if s.checkAndRegisterSession(ctx, stickyAcc, sessionHash) {
						return &AccountSelectionResult{
							Account:     stickyAcc,
							Acquired:    true,
							ReleaseFunc: slotResult.ReleaseFunc,
						}, nil, true
					}
					slotResult.ReleaseFunc()
				} else if s.concurrencyService != nil {
					waitingCount, _ := s.concurrencyService.GetAccountWaitingCount(ctx, stickyAccountID)
				if waitingCount < cfg.StickySessionMaxWaiting {
					if s.checkAndRegisterSession(ctx, stickyAcc, sessionHash) {
						return &AccountSelectionResult{
							Account: stickyAcc,
							WaitPlan: &AccountWaitPlan{
								AccountID:      stickyAccountID,
								MaxConcurrency: stickyAcc.Concurrency,
								Timeout:        cfg.StickySessionWaitTimeout,
								MaxWaiting:     cfg.StickySessionMaxWaiting,
							},
						}, nil, true
					}
				}
			}
		}
	}

	// 获取路由候选负载，按 priority ASC 串行拿槽
	routingLoads := make([]AccountWithConcurrency, 0, len(routingCandidates))
	for _, acc := range routingCandidates {
		routingLoads = append(routingLoads, AccountWithConcurrency{
			ID:             acc.ID,
			MaxConcurrency: acc.EffectiveLoadFactor(),
		})
	}
	routingLoadMap, _ := s.concurrencyService.GetAccountsLoadBatch(ctx, routingLoads)

	var available []accountWithLoad
	for _, acc := range routingCandidates {
		loadInfo := routingLoadMap[acc.ID]
		if loadInfo == nil {
			loadInfo = &AccountLoadInfo{AccountID: acc.ID}
		}
		if loadInfo.LoadRate < 100 {
			available = append(available, accountWithLoad{account: acc, loadInfo: loadInfo})
		}
	}

	sort.SliceStable(available, func(i, j int) bool {
		a, b := available[i], available[j]
		if a.account.Priority != b.account.Priority {
			return a.account.Priority < b.account.Priority
		}
		return a.loadInfo.LoadRate < b.loadInfo.LoadRate
	})

	for _, item := range available {
		slotResult, slotErr := s.tryAcquireAccountSlot(ctx, item.account.ID, item.account.Concurrency)
		if slotErr == nil && slotResult.Acquired {
			if !s.checkAndRegisterSession(ctx, item.account, sessionHash) {
				slotResult.ReleaseFunc()
				continue
			}
			if sessionHash != "" && s.cache != nil {
				_, _ = s.cache.SetSessionAccountIDIfBetter(ctx, derefGroupID(groupID), sessionHash, item.account.ID, item.account.Priority, stickySessionTTL)
			}
			return &AccountSelectionResult{
				Account:     item.account,
				Acquired:    true,
				ReleaseFunc: slotResult.ReleaseFunc,
			}, nil, true
		}
	}

	// 所有路由账号槽位满 → WaitPlan（选 priority 最小的）
	for _, item := range available {
		if s.checkAndRegisterSession(ctx, item.account, sessionHash) {
			return &AccountSelectionResult{
				Account: item.account,
				WaitPlan: &AccountWaitPlan{
					AccountID:      item.account.ID,
					MaxConcurrency: item.account.Concurrency,
					Timeout:        cfg.StickySessionWaitTimeout,
					MaxWaiting:     cfg.StickySessionMaxWaiting,
				},
			}, nil, true
		}
	}

	// 路由账号全不可用（所有账号 load 100% 或会话限制满），回退到 Layer 2
	return nil, nil, false
}

