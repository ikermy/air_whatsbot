package whatsapp

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"air_whatsbot/internal/metrics"

	"github.com/ikermy/air-common/pkg/comdom"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow/types"
	_ "modernc.org/sqlite"
)

func (b *Bot) initializeUserChannels(senderID uint64, senderName string) error {
	startedAt := time.Now()
	status := "success"
	defer func() {
		metrics.ObserveDuration(metrics.UserChannelInitDuration.WithLabelValues(metrics.BotLabel(b.userID), status), startedAt)
	}()

	if err := validateResponder(b.userID, senderID); err != nil {
		status = "error"
		return err
	}

	if err := initUserChannels(
		b.ctx,
		b.userID,
		senderID,
		senderName,
		b.db,
		b.mod,
		b.assist,
		func(respID uint64, usrCh *model.Ch) {
			b.startResponseListener(respID, usrCh)
		},
		func(start model.StartCh) error {
			select {
			case b.parent.startCh <- start:
				return nil
			default:
				return errors.New("ошибка отправки данных в StartCh")
			}
		},
	); err != nil {
		status = "error"
		return err
	}

	return nil
}

// startResponseListener запускает горутину для прослушивания ответов ассистента
func (b *Bot) startResponseListener(respId uint64, usrCh *model.Ch) {
	var jid types.JID
	var realPhone string

	respInfo, exists := b.responders.Load(respId)
	if exists && respInfo != nil {
		jid = respInfo.JID
		realPhone = respInfo.RealPhone
	}
	if realPhone == "" {
		realPhone = strconv.FormatUint(respId, 10)
	}
	if jid.User == "" {
		var err error
		jid, err = types.ParseJID(realPhone + "@s.whatsapp.net")
		if err != nil {
			logger.Error("Ошибка создания JID для пользователя %d: %v", respId, err, b.userID)
			return
		}
		logger.Warn("JID респондента не найден для %d, используем %s", respId, jid.String(), b.userID)
	}

	logger.Debug("Используем JID для отправки: %s, телефон для CRM: %s (respId: %d)",
		jid.String(), realPhone, respId, b.userID)

	runResponseListener(b.ctx, respId, usrCh,
		func(msg model.Message) error {
			logger.Debug("Сообщение ассистента %v", msg)
			if err := b.sendMessage(jid, msg); err != nil {
				// Ошибка доставки в WhatsApp не должна мешать уведомить CRM.
				metrics.WhatsAppSend.WithLabelValues(metrics.BotLabel(b.userID), "error").Inc()
				logger.Error("Ошибка отправки сообщения пользователю %d: %v", respId, err, b.userID)
			} else {
				metrics.WhatsAppSend.WithLabelValues(metrics.BotLabel(b.userID), "success").Inc()
			}
			logger.Info("Отправка в CRM с телефоном: %s", realPhone, b.userID)
			csg := b.c.MSG("assist", usrCh.RespName, msg.Content.Message).
				WithPhone(realPhone).
				WithFiles(func() []string {
					var f []string
					for _, file := range msg.Content.Action.SendFiles {
						f = append(f, file.FileName)
					}
					return f
				}()...).
				SetMeta(msg.Content.Meta)
			crmStartedAt := time.Now()
			if err := b.c.SendMessage(csg); err != nil {
				metrics.CRMRequests.WithLabelValues(metrics.BotLabel(b.userID), "outgoing", "error").Inc()
				metrics.ObserveDuration(metrics.CRMRequestDuration.WithLabelValues(metrics.BotLabel(b.userID), "outgoing"), crmStartedAt)
				logger.Error("Ошибка отправки ответа ассистента в CRM: %v", err, b.userID)
				return err
			}
			metrics.CRMRequests.WithLabelValues(metrics.BotLabel(b.userID), "outgoing", "success").Inc()
			metrics.ObserveDuration(metrics.CRMRequestDuration.WithLabelValues(metrics.BotLabel(b.userID), "outgoing"), crmStartedAt)
			return nil
		},
		func(dialogID uint64, enabled bool) {
			if enabled {
				b.parent.SetOperatorMode(dialogID, true)
				metrics.TrackOperatorModeDialogs(b.userID, countSyncMap(&b.parent.operatorModeByDialog))
				logger.Debug("Включен режим оператора для диалога %d", dialogID, b.userID)
			}
		},
		func(code int) {
			metrics.MessagesProcessed.WithLabelValues(metrics.BotLabel(b.userID), "tx_channel_closed").Inc()
			logger.Error("Канал TxCh закрыт для пользователя %d", respId, b.userID)
		},
	)
}

// dialogResolver — порт получения/создания диалога.
type dialogResolver interface {
	GetOrSetTreadAndResponder(userID uint32, responderRealId uint64, responderName string, chatType comdom.ChannelType) (uint64, error)
}

func validateResponder(userID uint32, senderID uint64) error {
	if userID == 0 {
		return fmt.Errorf("invalid userId: %d", userID)
	}
	if senderID == 0 {
		return fmt.Errorf("invalid senderID: %d", senderID)
	}
	return nil
}

func dialogIDForUser(userID uint32, senderID uint64, senderName string, db dialogResolver) (uint64, error) {
	if userID == 0 {
		return 0, fmt.Errorf("invalid userId: %d", userID)
	}
	if senderID == 0 {
		return 0, fmt.Errorf("invalid senderID: %d", senderID)
	}
	if db == nil {
		return 0, fmt.Errorf("dialog resolver is nil")
	}

	dialogID, err := db.GetOrSetTreadAndResponder(userID, senderID, senderName, comdom.WhatsApp)
	if err != nil {
		return 0, fmt.Errorf("ошибка ID диалога: %w", err)
	}
	return dialogID, nil
}

// initUserChannels инициализирует модель, канал и диалог для респондента.
func initUserChannels(
	ctx context.Context,
	userID uint32,
	senderID uint64,
	senderName string,
	db dialogResolver,
	mod model.Inter,
	assist *model.Assistant,
	onListen func(uint64, *model.Ch),
	onStart func(model.StartCh) error,
) error {
	if userID == 0 {
		return fmt.Errorf("invalid userId: %d", userID)
	}
	if senderID == 0 {
		return fmt.Errorf("invalid senderID: %d", senderID)
	}
	if db == nil {
		return fmt.Errorf("dialog resolver is nil")
	}
	if mod == nil {
		return fmt.Errorf("model provider is nil")
	}
	if assist == nil {
		return fmt.Errorf("assistant is nil")
	}

	dialogID, err := dialogIDForUser(userID, senderID, senderName, db)
	if err != nil {
		return err
	}

	existingCh, existingErr := mod.GetCh(senderID)
	if existingErr == nil && existingCh.IsRxOpen() {
		return nil
	}
	if existingErr == nil {
		mod.CleanDialogData(dialogID)
	}

	usrMod, err := mod.GetOrSetRespGPT(*assist, dialogID, senderID, senderName)
	if err != nil && !strings.Contains(err.Error(), "получены пустые данные") {
		return fmt.Errorf("ошибка модели пользователя: %w", err)
	}

	usrCh, err := mod.GetCh(senderID)
	if err != nil && !strings.Contains(err.Error(), "получены пустые данные") {
		return fmt.Errorf("ошибка канала пользователя: %w", err)
	}

	startCh := model.StartCh{
		Ctx:      ctx,
		ChName:   comdom.WhatsApp,
		Model:    usrMod,
		Channel:  usrCh,
		ThreadId: dialogID,
		RespId:   senderID,
	}

	if onListen != nil {
		onListen(senderID, usrCh)
	}
	if onStart != nil {
		if err := onStart(startCh); err != nil {
			return err
		}
	}

	return nil
}

// runResponseListener читает ответы ассистента и прокидывает их в колбэки.
func runResponseListener(
	ctx context.Context,
	respId uint64,
	usrCh *model.Ch,
	onAssist func(model.Message) error,
	onOperator func(dialogID uint64, enabled bool),
	onMetric func(int),
) {
	if usrCh == nil {
		return
	}
	go func() {
		for {
			select {
			case msg, ok := <-usrCh.TxCh:
				if !ok {
					if onMetric != nil {
						onMetric(0)
					}
					logger.Error("Канал TxCh закрыт для пользователя %d", respId)
					return
				}
				if msg.Operator.Operator && msg.Operator.SetOperator {
					if onOperator != nil {
						onOperator(usrCh.DialogID, true)
					}
				}
				if msg.Type == "assist" {
					if onAssist != nil {
						if err := onAssist(msg); err != nil {
							logger.Error("Ошибка обработки assist-ответа для respId=%d: %v", respId, err)
						}
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
