package whatsapp

import (
	"air_whatsbot/internal/metrics"
	messagespkg "air_whatsbot/internal/whatsapp/messages"
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

func (b *Bot) extractFilesFromMessage(msg *events.Message) []model.FileUpload {
	return messagespkg.ExtractFilesFromMessage(msg, b.ctx, func(ctx context.Context, media whatsmeow.DownloadableMessage) ([]byte, error) {
		return b.b.Download(ctx, media)
	})
}

func (b *Bot) sendMessage(jid types.JID, message model.Message) error {
	startedAt := time.Now()
	defer metrics.ObserveDuration(metrics.WhatsAppSendDuration.WithLabelValues(metrics.BotLabel(b.userID)), startedAt)

	// senderID нужен только для отмены статуса печатания. Для @lid JID он
	// нечисловой, поэтому ошибку парсинга игнорируем (в этом случае отмены не будет).
	senderID, _ := strconv.ParseUint(jid.User, 10, 64)

	logger.Debug("Отправка сообщения на JID: %s (userID: %d)", jid.String(), senderID, b.userID)

	// Отменяем статус печатания в defer, чтобы гарантировать выполнение
	defer func() {
		// Получаем и отменяем контекст печатания
		cancelTyping := b.getAndRemoveTypingCancel(senderID)
		if cancelTyping != nil {
			cancelTyping()
			logger.Debug("Контекст печатания отменен для %d", senderID, b.userID)
		}
		// Устанавливаем статус "пауза"
		if err := b.b.SendChatPresence(b.ctx, jid, types.ChatPresencePaused, types.ChatPresenceMediaText); err != nil {
			logger.Debug("Ошибка установки статуса паузы: %v", err, b.userID)
		} else {
			logger.Debug("Статус 'пауза' установлен для %d", senderID, b.userID)
		}
	}()

	// Сначала отправляем текстовое сообщение, если оно есть
	if message.Content.Message != "" {
		logger.Debug("Отправка текста на JID: %s", jid.String(), b.userID)
		_, err := b.b.SendMessage(b.ctx, jid, &waE2E.Message{
			Conversation: proto.String(message.Content.Message),
		})
		if err != nil {
			return fmt.Errorf("ошибка отправки текста: %w", err)
		}
		logger.Debug("Текст успешно отправлен на JID: %s", jid.String(), b.userID)
	}

	// Затем отправляем файлы из Action.SendFiles
	for _, file := range message.Content.Action.SendFiles {
		err := b.sendFile(jid, file)
		if err != nil {
			logger.Error("Ошибка отправки файла %s: %v", file.FileName, err, b.userID)
			continue
		}
	}

	return nil
}

func (b *Bot) sendFile(jid types.JID, file model.File) error {
	// Получаем файл с повторными попытками
	data, err := b.uploadFileWithRetry(file.URL, file.FileName)
	if err != nil {
		return fmt.Errorf("ошибка получения файла %s: %w", file.FileName, err)
	}

	// Определяем MIME-тип
	mimeType := b.getMimeTypeFromFilename(file.FileName)

	// Загружаем файл в WhatsApp с правильным типом медиа
	mediaType := b.getWhatsAppMediaType(file.Type)
	uploaded, err := b.b.Upload(b.ctx, data, mediaType)
	if err != nil {
		return fmt.Errorf("ошибка загрузки файла в WhatsApp: %w", err)
	}

	// Отправляем в зависимости от типа файла
	switch file.Type {
	case model.Photo:
		_, err = b.b.SendMessage(b.ctx, jid, &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{
				Caption:       proto.String(file.Caption),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
			},
		})
	case model.Video:
		_, err = b.b.SendMessage(b.ctx, jid, &waE2E.Message{
			VideoMessage: &waE2E.VideoMessage{
				Caption:       proto.String(file.Caption),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
			},
		})
	case model.Audio:
		_, err = b.b.SendMessage(b.ctx, jid, &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
			},
		})
	case model.Doc:
		_, err = b.b.SendMessage(b.ctx, jid, &waE2E.Message{
			DocumentMessage: &waE2E.DocumentMessage{
				FileName:      proto.String(file.FileName),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
			},
		})
	default:
		return fmt.Errorf("неподдерживаемый тип файла: %s", file.Type)
	}

	if err != nil {
		return fmt.Errorf("ошибка отправки файла %s: %w", file.FileName, err)
	}

	return nil
}

func (b *Bot) uploadFileWithRetry(fileURL, fileName string) ([]byte, error) {
	maxRetries := 3
	baseDelay := time.Second

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(b.ctx, "GET", fileURL, nil)
		if err != nil {
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("ошибка создания запроса: %w", err)
			}
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		// Устанавливаем User-Agent для избежания блокировок
		req.Header.Set("User-Agent", "WhatsApp-Bot/1.0")

		resp, err := client.Do(req)
		if err != nil {
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("ошибка выполнения запроса: %w", err)
			}
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		if resp.StatusCode == 415 {
			_ = resp.Body.Close()
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("сервер не поддерживает тип файла после %d попыток", maxRetries)
			}
			logger.Warn("Попытка %d: получен статус 415 для файла %s, повторяем через %v", attempt+1, fileName, baseDelay*time.Duration(attempt+1))
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("HTTP ошибка: %d %s", resp.StatusCode, resp.Status)
			}
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("ошибка чтения данных: %w", err)
			}
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		return data, nil
	}

	return nil, fmt.Errorf("все попытки загрузки файла исчерпаны")
}

func (b *Bot) getMimeTypeFromFilename(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".mp4":
		return "video/mp4"
	case ".mp3":
		return "audio/mpeg"
	case ".ogg":
		return "audio/ogg"
	case ".pdf":
		return "application/pdf"
	case ".txt":
		return "text/plain"
	default:
		return "application/octet-stream"
	}
}

func (b *Bot) getWhatsAppMediaType(fileType model.FileType) whatsmeow.MediaType {
	switch fileType {
	case model.Photo:
		return whatsmeow.MediaImage
	case model.Video:
		return whatsmeow.MediaVideo
	case model.Audio:
		return whatsmeow.MediaAudio
	case model.Doc:
		return whatsmeow.MediaDocument
	default:
		return whatsmeow.MediaDocument
	}
}
