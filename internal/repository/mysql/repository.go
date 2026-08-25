package mysql

import (
	"air_whatsbot/internal/domain"
	"air_whatsbot/internal/repository"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ikermy/air-common/pkg/comdb"
	"github.com/ikermy/air-common/pkg/comdom"
	"github.com/ikermy/air-common/pkg/crypto"
	"github.com/ikermy/air-common/pkg/mode"
	"github.com/ikermy/air-common/pkg/model/create"
)

type Implementation struct {
	db *comdb.DB
}

func New(db *comdb.DB) (repository.Repository, error) {
	if db == nil {
		return repository.Repository{}, fmt.Errorf("database connection is nil")
	}

	repo := &Implementation{db: db}

	return repository.Repository{
		Intermal: repo,
		External: db,
	}, nil
}

// NullBytes Промежуточный тип для загрузки массива байт из базы.
type NullBytes struct {
	Bytes []byte
	Valid bool
}

// Scan реализует интерфейс sql.Scanner.
func (nb *NullBytes) Scan(value any) error {
	if value == nil {
		nb.Bytes = nil
		nb.Valid = false
		return nil
	}

	switch v := value.(type) {
	case []byte:
		nb.Bytes = v
		nb.Valid = true
	default:
		return fmt.Errorf("cannot scan type %T into NullBytes", value)
	}
	return nil
}

func (r *Implementation) UpdateWhatsBotData(ctx context.Context, userId uint32, mk [32]byte, data string, enabled bool) error {
	if userId == 0 {
		return fmt.Errorf("получен некорректный userId: %d", userId)
	}

	ctx, cancel := context.WithTimeout(ctx, mode.GetSQLTimeToCancel())
	defer cancel()

	tx, err := r.db.Conn().BeginTx(ctx, nil)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return fmt.Errorf("тайм-аут (%d с) при начале транзакции: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return fmt.Errorf("операция отменена при начале транзакции: %w", err)
		default:
			return fmt.Errorf("ошибка начала транзакции: %w", err)
		}
	}
	defer func() { _ = tx.Rollback() }()

	var foundUserId sql.NullInt32
	err = tx.QueryRowContext(ctx, "SELECT UserId FROM channels WHERE UserId = ? LIMIT 1", userId).Scan(&foundUserId)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return fmt.Errorf("тайм-аут (%d с) при проверке существования записи: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return fmt.Errorf("операция отменена при проверке существования записи: %w", err)
		default:
			return fmt.Errorf("ошибка проверки существования записи: %w", err)
		}
	}

	if errors.Is(err, sql.ErrNoRows) || !foundUserId.Valid {
		return fmt.Errorf("запись для пользователя %d не найдена в таблице channels", userId)
	}

	// Шифруем данные канала MasterKey'ом ($mk$) если он доступен
	var zeroKey [32]byte
	if !bytes.Equal(mk[:], zeroKey[:]) {
		encrypted, err := crypto.EncryptFieldWithMasterKey(mk, data)
		if err != nil {
			return fmt.Errorf("failed to encrypt channel data with MasterKey: %w", err)
		}
		data = encrypted
	}

	enabledInt := 0
	if enabled {
		enabledInt = 1
	}

	_, err = tx.ExecContext(ctx, `
  UPDATE channels
  SET Whats = ?, Whats_enabled = ?
  WHERE UserId = ?`, data, enabledInt, userId)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return fmt.Errorf("тайм-аут (%d с) при обновлении данных WhatsApp бота: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return fmt.Errorf("операция отменена при обновлении данных WhatsApp бота: %w", err)
		default:
			return fmt.Errorf("ошибка обновления данных WhatsApp бота: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return fmt.Errorf("тайм-аут (%d с) при коммите транзакции: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return fmt.Errorf("операция отменена при коммите транзакции: %w", err)
		default:
			return fmt.Errorf("ошибка коммита транзакции: %w", err)
		}
	}

	return nil
}

func (r *Implementation) GetWaUserBotUsers(ctx context.Context) ([]domain.WaUserBotData, error) {
	ctx, cancel := context.WithTimeout(ctx, mode.GetSQLTimeToCancel())
	defer cancel()

	query := `
		SELECT 
			c.UserId,
			c.Whats,
			c.Whats_enabled,
			u_gpt.Name,
			u_gpt.AssistantId,
			u_gpt.Data,
			um.Provider,
			n.Start,
			n.End,
			n.Target
		FROM 
			channels AS c
		LEFT JOIN 
			user_models AS um ON c.UserId = um.UserId AND um.IsActive = 1
		LEFT JOIN 
			user_gpt AS u_gpt ON um.ModelId = u_gpt.Id
		LEFT JOIN
			notifications AS n ON c.UserId = n.UserId
		WHERE 
			c.Whats IS NOT NULL AND c.Whats_enabled = 1`

	rows, err := r.db.Conn().QueryContext(ctx, query)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("тайм-аут (%d с) при получении пользователей WaUserBot: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return nil, fmt.Errorf("операция отменена при получении пользователей WaUserBot: %w", err)
		default:
			return nil, fmt.Errorf("failed to execute stored procedure: %w", err)
		}
	}
	defer func() { _ = rows.Close() }()

	var users []domain.WaUserBotData
	for rows.Next() {
		user, err := scanWaUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, *user)
	}

	if err = rows.Err(); err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("тайм-аут (%d с) при обработке результатов пользователей WaUserBot: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return nil, fmt.Errorf("операция отменена при обработке результатов пользователей WaUserBot: %w", err)
		default:
			return nil, fmt.Errorf("error iterating rows: %w", err)
		}
	}

	return users, nil
}

func (r *Implementation) GetWaUser(ctx context.Context, userId uint32) (*domain.WaUserBotData, error) {
	if userId == 0 {
		return nil, fmt.Errorf("получен некорректный userId: %d", userId)
	}

	ctx, cancel := context.WithTimeout(ctx, mode.GetSQLTimeToCancel())
	defer cancel()

	query := `
		SELECT 
			c.UserId,
			c.Whats,
			c.Whats_enabled,
			u_gpt.Name,
			u_gpt.AssistantId,
			u_gpt.Data,
			um.Provider,
			n.Start,
			n.End,
			n.Target
		FROM 
			channels AS c
		JOIN 
			users AS u ON c.UserId = u.Id
		LEFT JOIN 
			user_models AS um ON u.Id = um.UserId AND um.IsActive = 1
		LEFT JOIN 
			user_gpt AS u_gpt ON um.ModelId = u_gpt.Id
		LEFT JOIN
			notifications AS n ON u.Id = n.UserId
		WHERE 
			c.UserId = ? AND c.Whats IS NOT NULL`

	var user domain.WaUserBotData
	var name, assistantId sql.NullString
	var provider sql.NullByte
	var data NullBytes
	var start, end, target sql.NullBool

	err := r.db.Conn().QueryRowContext(ctx, query, userId).Scan(
		&user.UserId,
		&user.SessionData,
		&user.WaUserBotEnabled,
		&name,
		&assistantId,
		&data,
		&provider,
		&start,
		&end,
		&target,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("пользователь с userId=%d не найден или не имеет настроек WhatsApp", userId)
		}
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("тайм-аут (%d с) при получении данных пользователя: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return nil, fmt.Errorf("операция отменена при получении данных пользователя: %w", err)
		default:
			return nil, fmt.Errorf("ошибка получения данных пользователя: %w", err)
		}
	}

	fillWaUserFields(&user, name, assistantId, provider, data, start, end, target)
	return &user, nil
}

func scanWaUser(rows *sql.Rows) (*domain.WaUserBotData, error) {
	var user domain.WaUserBotData
	var name, assistantId sql.NullString
	var provider sql.NullByte
	var data NullBytes
	var start, end, target sql.NullBool

	err := rows.Scan(
		&user.UserId,
		&user.SessionData,
		&user.WaUserBotEnabled,
		&name,
		&assistantId,
		&data,
		&provider,
		&start,
		&end,
		&target,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	fillWaUserFields(&user, name, assistantId, provider, data, start, end, target)
	return &user, nil
}

func fillWaUserFields(user *domain.WaUserBotData, name, assistantId sql.NullString, provider sql.NullByte, data NullBytes, start, end, target sql.NullBool) {
	if assistantId.Valid {
		user.AssistantId = assistantId.String
	}
	if name.Valid {
		user.AssistName = name.String
	}
	if provider.Valid {
		user.Provider = comdom.ProviderType(provider.Byte)
	} else {
		user.Provider = comdom.ProviderOpenAI
	}
	if data.Valid {
		mdata, err := create.DecompressModelData(data.Bytes)
		if err == nil {
			user.MetaAction = mdata.MetaAction
			user.Triggers = mdata.Triggers
			user.AskLimit = uint32(mdata.Espero.Limit)
			user.Espero = mdata.Espero.Wait
			user.Ignore = mdata.Espero.Ignore
		}
	}
	if start.Valid {
		user.Events.Start = start.Bool
	}
	if end.Valid {
		user.Events.End = end.Bool
	}
	if target.Valid {
		user.Events.Target = target.Bool
	}
}
