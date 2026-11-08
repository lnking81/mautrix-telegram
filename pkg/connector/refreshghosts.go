// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 Ilya Strukov
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package connector

import (
	"context"
	"fmt"
	"slices"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/format"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

// cmdRefreshGhosts re-fetches the Telegram profile of every direct chat partner of a login and
// applies it to their ghost.
//
// bridgev2 refreshes a ghost only while its name or avatar is missing
// (Ghost.UpdateInfoIfNecessary), so ghosts created before the connector reported a field never get
// it — e.g. IsBot, which bridgev2 publishes as com.beeper.bridge.is_network_bot in the ghost's member
// events, and the telegram:/tel: identifiers. Opt-in: nothing changes until the command is run.
var cmdRefreshGhosts = &commands.FullHandler{
	Func: fnRefreshGhosts,
	Name: "refresh-ghosts",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionChats,
		Description: "Re-fetch the Telegram profiles of your direct chat partners (bot flag, usernames, phone numbers)",
		Args:        "[_login ID_]",
	},
	RequiresLogin: true,
}

// users.getUsers accepts up to 100 users per call.
const refreshGhostsBatchSize = 100

func fnRefreshGhosts(ce *commands.Event) {
	logins := ce.User.GetUserLogins()
	if len(ce.Args) > 0 {
		logins = slices.DeleteFunc(logins, func(login *bridgev2.UserLogin) bool {
			return !slices.Contains(ce.Args, string(login.ID))
		})
		if len(logins) == 0 {
			ce.Reply("No matching logins found with provided ID(s)")
			return
		}
	}
	for _, login := range logins {
		client := login.Client.(*TelegramClient)
		refreshed, bots, err := client.refreshDMGhosts(ce.Ctx)
		if err != nil {
			ce.Reply("Failed to refresh ghosts for %s after %d profiles: %v",
				format.SafeMarkdownCode(login.ID), refreshed, err)
		} else {
			ce.Reply("Refreshed %d direct chat partners for %s (%d of them bots)",
				refreshed, format.SafeMarkdownCode(login.ID), bots)
		}
	}
}

// refreshDMGhosts fetches the partners of all of this login's DM portals in batches and runs
// Ghost.UpdateInfo with the full user info. Returns how many ghosts were updated and how many of
// them are bots.
func (tc *TelegramClient) refreshDMGhosts(ctx context.Context) (refreshed, bots int, err error) {
	log := zerolog.Ctx(ctx).With().Str("action", "refresh dm ghosts").Logger()
	userPortals, err := tc.main.Bridge.DB.UserPortal.GetAllForLogin(ctx, tc.userLogin.UserLogin)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get portals of login: %w", err)
	}
	seen := make(map[int64]struct{})
	var inputs []tg.InputUserClass
	for _, up := range userPortals {
		peerType, userID, _, err := ids.ParsePortalID(up.Portal.ID)
		if err != nil || peerType != ids.PeerTypeUser || userID == tc.telegramUserID {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		input, err := tc.getInputUser(ctx, userID)
		if err != nil {
			log.Warn().Err(err).Int64("user_id", userID).Msg("Can't address DM partner, skipping")
			continue
		}
		inputs = append(inputs, input)
	}
	log.Info().Int("partners", len(inputs)).Msg("Refreshing DM partner profiles")
	for batch := range slices.Chunk(inputs, refreshGhostsBatchSize) {
		users, err := tc.client.API().UsersGetUsers(ctx, batch)
		if err != nil {
			return refreshed, bots, fmt.Errorf("users.getUsers: %w", err)
		}
		for _, rawUser := range users {
			user, ok := rawUser.(*tg.User)
			if !ok {
				continue
			}
			ghost, err := tc.main.Bridge.GetGhostByID(ctx, ids.MakeUserID(user.ID))
			if err != nil {
				return refreshed, bots, fmt.Errorf("failed to get ghost for %d: %w", user.ID, err)
			}
			info, err := tc.wrapUserInfo(ctx, user, ghost)
			if err != nil {
				log.Warn().Err(err).Int64("user_id", user.ID).Msg("Failed to convert user info, skipping")
				continue
			}
			ghost.UpdateInfo(ctx, info)
			refreshed++
			if user.Bot {
				bots++
			}
		}
	}
	log.Info().Int("refreshed", refreshed).Int("bots", bots).Msg("Refreshed DM partner profiles")
	return refreshed, bots, nil
}
