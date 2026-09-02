import re



TRADE_ID_PATTERN = re.compile(
    r"\bTRD-[A-Z0-9]+(?:-[A-Z0-9]+)*\b",
    re.IGNORECASE,
)

def extract_trade_id(question: str) -> str | None:
    match = TRADE_ID_PATTERN.search(question)

    if not match:
        return None

    return match.group(0).upper()


def main():
    from intelligence.rag import answer_question

    print("FinMesh Intelligence")
    print("Type 'exit' to quit.\n")

    while True:
        question = input("FinMesh> ").strip()

        if question.lower() in {"exit", "quit"}:
            break

        if not question:
            continue

        try:
            answer = answer_question(question)
            print(f"\n{answer}\n")

        except Exception as exc:
            print(f"\nQuery failed: {exc}\n")


if __name__ == "__main__":
    main()