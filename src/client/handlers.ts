import type { ConfirmChannel } from "amqplib";
import type {
  ArmyMove,
  RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";
import type {
  GameState,
  PlayingState,
} from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilTopic,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { publishGameLog } from "./index.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState): AckType => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(
  gs: GameState,
  ch: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove): Promise<AckType> => {
    try {
      const outcome = handleMove(gs, move);
      switch (outcome) {
        case MoveOutcome.Safe:
        case MoveOutcome.SamePlayer:
          return AckType.Ack;
        case MoveOutcome.MakeWar:
          const recognition: RecognitionOfWar = {
            attacker: move.player,
            defender: gs.getPlayerSnap(),
          };

          try {
            await publishJSON(
              ch,
              ExchangePerilTopic,
              `${WarRecognitionsPrefix}.${gs.getUsername()}`,
              recognition,
            );
            return AckType.Ack;
          } catch (err) {
            console.error("Error publishing war recognition:", err);
            return AckType.NackRequeue;
          }
        default:
          return AckType.NackDiscard;
      }
    } finally {
      process.stdout.write("> ");
    }
  };
}

export function handlerWar(
  gs: GameState,
  ch: ConfirmChannel,
): (war: RecognitionOfWar) => Promise<AckType> {
  return async (war: RecognitionOfWar): Promise<AckType> => {
    try {
      const outcome = handleWar(gs, war);

      switch (outcome.result) {
        case WarOutcome.NotInvolved:
          return AckType.NackRequeue;
        case WarOutcome.NoUnits:
          return AckType.NackDiscard;
        case WarOutcome.YouWon:
          try {
            await publishGameLog(
              ch,
              gs.getUsername(),
              `${outcome.winner} won the war against ${outcome.loser}.`,
            );
          } catch (err) {
            console.error("Error publishing game log:", err);
            return AckType.NackRequeue;
          }
          return AckType.Ack;
        case WarOutcome.OpponentWon:
          try {
            await publishGameLog(
              ch,
              gs.getUsername(),
              `${outcome.winner} won the war against ${outcome.loser}.`,
            );
          } catch (err) {
            console.error("Error publishing game log:", err);
            return AckType.NackRequeue;
          }
          return AckType.Ack;
        case WarOutcome.Draw:
          try {
            await publishGameLog(
              ch,
              gs.getUsername(),
              `A war between ${outcome.attacker} and ${outcome.defender} resulted in a draw.`,
            );
          } catch (err) {
            console.error("Error publishing game log:", err);
            return AckType.NackRequeue;
          }
          return AckType.Ack;
        default:
          const unreachable: never = outcome;
          console.log("Unexpected war resolution: ", unreachable);
          return AckType.NackDiscard;
      }
    } finally {
      process.stdout.write("> ");
    }
  };
}
