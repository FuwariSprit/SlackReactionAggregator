import { stringify } from "@std/csv";

// envの値を取得
const SLACK_TOKEN = Deno.env.get("SLACK_TOKEN");
const CHANNEL_ID = Deno.env.get("SLACK_CHANNEL_ID");
const YEARS_AGO = Number(Deno.env.get("YEARS_AGO")) || 1;
const MESSAGES_FILE_NAME = Deno.env.get("MESSAGES_FILE_NAME") || "messages.csv";
const REACTIONS_FILE_NAME = Deno.env.get("REACTIONS_FILE_NAME") ||
  "reactions.csv";

if (!SLACK_TOKEN) {
  console.error("SLACK_TOKEN is not set");
  Deno.exit(1);
}

if (!CHANNEL_ID) {
  console.error("SLACK_CHANNEL_ID is not set");
  Deno.exit(1);
}

// 今日からn年前の月初めのUnixタイムスタンプを取得
const MONTH_START_PREV_YEARS = Math.floor(
  new Date(new Date().getFullYear() - YEARS_AGO, new Date().getMonth(), 1)
    .getTime() / 1000,
);

// 今日の00:00のUnixタイムスタンプを取得
const TODAY = Math.floor(new Date().setHours(0, 0, 0, 0) / 1000);

// ページング処理用の変数
let cursor: string | null = null;

// メッセージとリアクションのデータを一時的に保存する配列
const messages: Array<[string, string, string]> = [];
const reactions: Array<[string, string, string]> = [];

/**
 * スリープ
 *
 * @param ms
 * @returns Promise<void>
 */
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * メッセージを取得
 *
 * @returns Promise<void>
 */
async function fetchMessages() {
  let hasMore = true;

  while (hasMore) {
    const url = new URL("https://slack.com/api/conversations.history");
    url.searchParams.set("channel", CHANNEL_ID!);
    url.searchParams.set("latest", TODAY.toString());
    url.searchParams.set("oldest", MONTH_START_PREV_YEARS.toString());
    url.searchParams.set("limit", "100");
    if (cursor) url.searchParams.set("cursor", cursor);

    console.log("url:" + url.toString());

    try {
      const response = await fetch(url.toString(), {
        method: "GET",
        headers: {
          "Authorization": `Bearer ${SLACK_TOKEN}`,
          "Content-Type": "application/json",
        },
      });

      if (!response.ok) {
        console.error(`HTTP Error: ${response.status}`);
        const errorResponse = await response.json();
        console.error("Error response:", errorResponse);
        return;
      }

      const data = await response.json();
      data.messages.forEach(
        (
          message: {
            ts: string;
            text: string;
            user: string;
            reactions?: { name: string; users: string[] }[];
          },
        ) => {
          const { ts, text, user, reactions: messageReactions = [] } = message;
          messages.push([ts, text, user]);

          messageReactions.forEach(
            (reaction: { name: string; users: string[] }) => {
              reaction.users.forEach((userId: string) => {
                reactions.push([ts, reaction.name, userId]);
              });
            },
          );
        },
      );

      cursor = data.response_metadata?.next_cursor || null;
      hasMore = !!cursor;

      await sleep(1200); // Tier3のAPIなので1.2秒のスリープを追加
    } catch (error) {
      console.error("Fetch error:", error);
    }
  }
}

/**
 * CSVファイルに書き込む
 *
 * @returns Promise<void>
 */
async function writeCSVFiles() {
  const messagesCSV = stringify([
    ["timestamp", "text", "user"],
    ...messages,
  ]);
  await Deno.writeTextFile(MESSAGES_FILE_NAME, messagesCSV);

  const reactionsCSV = stringify([[
    "timestamp",
    "reaction",
    "reacted_by",
  ], ...reactions]);
  await Deno.writeTextFile(REACTIONS_FILE_NAME, reactionsCSV);
}

// メイン処理
await fetchMessages();
await writeCSVFiles();
console.log(`Messages saved to ${MESSAGES_FILE_NAME}`);
console.log(`Reactions saved to ${REACTIONS_FILE_NAME}`);
