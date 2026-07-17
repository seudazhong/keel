import { fireEvent, render, screen } from "@testing-library/react";
import { expect, test } from "vitest";
import { I18nProvider } from "../../../lib/i18n";
import { DiffViewer } from "./DiffViewer";
import type { DiffFile } from "./types";

const files: DiffFile[] = [
  {
    path: "src/example.ts",
    additions: 2,
    deletions: 1,
    hunks: [
      {
        header: "@@ -1,1 +1,2 @@",
        lines: [
          { type: "context", text: "const a = 1;" },
          { type: "del", text: "const b = 2;" },
          { type: "add", text: "const b = 3;" },
          { type: "add", text: "const c = 4;" },
        ],
      },
    ],
  },
];

function renderViewer(data: DiffFile[]) {
  return render(
    <I18nProvider>
      <DiffViewer files={data} />
    </I18nProvider>,
  );
}

test("shows a compact collapsed summary per file with add/remove counts", () => {
  renderViewer(files);
  expect(screen.getByText("src/example.ts")).toBeInTheDocument();
  expect(screen.getByText("2 added")).toBeInTheDocument();
  expect(screen.getByText("1 removed")).toBeInTheDocument();
  expect(screen.queryByText("const b = 3;")).not.toBeInTheDocument();
});

test("expanding a file reveals its hunk lines with add/remove markers", () => {
  renderViewer(files);
  fireEvent.click(screen.getByRole("button", { name: /src\/example\.ts/ }));
  expect(screen.getByText(/const b = 3;/)).toBeInTheDocument();
  expect(screen.getByText(/const b = 2;/)).toBeInTheDocument();
});

test("renders an empty state when a run has no file changes", () => {
  renderViewer([]);
  expect(screen.getByText("No file changes reported for this run.")).toBeInTheDocument();
});
