import re

from intelligence.rag import answer_trade_question


TRADE_ID_PATTERN = re.compile(r"\bTRD-[A-Z0-9]+\b", re.IGNORECASE)


def extract_trade_id(question: str) -> str | None:
    match = TRADE_ID_PATTERN.search(question)

    if not match:
        return None

    return match.group(0).upper()


def main():
    print("FinMesh Intelligence")
    print("Type 'exit' to quit.\n")

    while True:
        question = input("FinMesh> ").strip()

        if question.lower() in {"exit", "quit"}:
            break

        trade_id = extract_trade_id(question)

        if not trade_id:
            print(
                "Please include a trade ID, for example: "
                "Why did TRD-C3E49666 fail?\n"
            )
            continue

        try:
            answer = answer_trade_question(
                trade_id=trade_id,
                question=question,
            )

            print(f"\n{answer}\n")

        except Exception as exc:
            print(f"\nRAG query failed: {exc}\n")


if __name__ == "__main__":
    main()