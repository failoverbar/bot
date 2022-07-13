package main

import (
	"context"
	"errors"
	"github.com/failoverbar/bot/model"
	"github.com/failoverbar/bot/wrap"
	ydbEnviron "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"log"
	"os"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	tele "gopkg.in/telebot.v3"
)

func main() {
	ctx := context.Background()
	dsn, ok := os.LookupEnv("YDB_DSN")
	if !ok {
		log.Fatal("Set env YDB_DSN and any cred from ydb-go-sdk-auth-environ")
	}
	db, err := ydb.Open(ctx, dsn, ydbEnviron.WithEnvironCredentials(ctx))
	if err != nil {
		log.Fatal("can't connect to DB", err)
	}
	settings := tele.Settings{
		Token:  os.Getenv("TELEGRAM_TOKEN"),
		Poller: &tele.LongPoller{Timeout: 10 * time.Second},
	}
	b, err := tele.NewBot(settings)
	if err != nil {
		log.Fatal(err)
	}
	b.Use(Logger(), AutoResponder)

	h := handler{
		bot:                 b,
		userRepo:            &model.UserRepo{DB: db},
		profileRepo:         &model.ProfileRepo{DB: db},
		telegramProfileRepo: &model.TelegramProfileRepo{DB: db},
		subscriptionsRepo:   &model.SubscriptionRepo{DB: db},
	}

	b.Handle("/start", h.onStart)

	b.Handle(tele.OnText, h.onText)

	b.Handle(tele.OnContact, h.onContact)

	// Сценарий регистрации
	// как зовут? Ты из айти? Кто ты в айти?
	// уведомления об интересных мероприятиях?
	b.Start()
}

type handler struct {
	bot *tele.Bot

	userRepo            *model.UserRepo
	profileRepo         *model.ProfileRepo
	telegramProfileRepo *model.TelegramProfileRepo
	subscriptionsRepo   *model.SubscriptionRepo
}

func (h *handler) onContact(c tele.Context) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if c.Message().Contact.UserID != c.Sender().ID {
		return c.Send("Получил контакт. Не знаю, что мне с ним делать, но очень интересно.")
	}
	userID := uint64(c.Sender().ID)
	profile, err := h.profileRepo.Get(ctx, userID)
	if err != nil {
		return err
	}
	profile.Phone = &c.Message().Contact.PhoneNumber
	if err := h.profileRepo.Upsert(ctx, profile); err != nil {
		return err
	}

	user, err := h.userRepo.Get(ctx, userID)
	if err != nil {
		return err
	}
	user.State = ""
	if err := h.userRepo.Upsert(ctx, user); err != nil {
		return err
	}

	m := h.bot.NewMarkup()
	m.Reply()
	m.RemoveKeyboard = true

	return c.Send("Благодарю. Позднее я попрошу тебя рассказать, какие ивенты тебе интересны.", m)
}

func (h *handler) onText(c tele.Context) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userID := uint64(c.Message().Sender.ID)
	user, err := h.userRepo.Get(ctx, userID)
	if err != nil && !errors.Is(err, wrap.NotFoundError{}) {
		return err
	}
	if err != nil {
		log.Printf("got text from not-existent user %d: %s", c.Message().Sender.ID, c.Message().Text)
		return h.onStart(c)
	}

	switch user.State {
	case "":
		log.Printf("got text with empty context %d: %s", c.Message().Sender.ID, c.Message().Text)
		return c.Send("Ничего не понятно, но очень интересно")
	case "register.name":
		return h.onTextRegisterName(c, ctx, user, c.Message().Text)
	case "register.phone":
		return h.onTextRegisterPhone(c, c.Message().Text)
	default:
		log.Printf("got unknown context %s from %d: %s", user.Context, c.Message().Sender.ID, c.Message().Text)
		return c.Send("А вы интересный человек")
	}
}

func (h *handler) onTextRegisterName(c tele.Context, ctx context.Context, user *model.User, msg string) error {
	profile, err := h.profileRepo.Get(ctx, user.UserID)
	if err != nil {
		return err
	}
	profile.Name = &msg
	if err := h.profileRepo.Upsert(ctx, profile); err != nil {
		return err
	}

	user.State = "register.phone"
	if err := h.userRepo.Upsert(ctx, user); err != nil {
		return err
	}

	return h.onTextRegisterPhone(c, *profile.Name)
}

func (h *handler) onTextRegisterPhone(c tele.Context, msg string) error {
	m := h.bot.NewMarkup()
	m.RemoveKeyboard = true
	m.Reply(m.Row(m.Contact("Отправить номер")))
	return c.Send("Очень приятно, "+msg+". Для доступа к WiFi и программе лояльности бара мне нужен твой телефон.\n\n"+
		"Обещаю никому его не раскрывать.", m)
}

func (h *handler) onStart(c tele.Context) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	userID := uint64(c.Message().Sender.ID)
	user, err := h.userRepo.Get(ctx, userID)
	if err != nil && !errors.Is(err, wrap.NotFoundError{}) {
		return err
	}
	if err == nil && user.State != "register" { // Reset state
		// TODO process payload
		user.State = ""
		if err := h.userRepo.Upsert(ctx, user); err != nil {
			return err
		}
		return c.Send("Бот переинициализирован")
	}
	user = &model.User{
		UserID: userID,
		State:  "register.name",
	}
	if err := h.userRepo.Insert(ctx, user); err != nil {
		return err
	}

	profile := &model.Profile{
		UserID: userID,
		Source: c.Message().Payload,
	}
	if err := h.profileRepo.Upsert(ctx, profile); err != nil {
		return err
	}

	tgProfile := &model.TelegramProfile{
		UserID:       userID,
		Username:     c.Sender().Username,
		FirstName:    c.Sender().FirstName,
		LastName:     c.Sender().LastName,
		LanguageCode: c.Sender().LanguageCode,
	}
	if err := h.telegramProfileRepo.Upsert(ctx, tgProfile); err != nil {
		return err
	}

	return c.Send("Тебя приветствует *бот Фейловер Бара*. 🤗 Давай знакомиться!\n\n*Как тебя зовут?*")
}
