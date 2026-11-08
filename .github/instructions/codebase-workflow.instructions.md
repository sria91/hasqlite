---
applyTo: '**'
---

## Codebase Memory MCP

**Use Codebase Memory MCP graph tools FIRST when available — before reading files or making code changes. Ordinary file search and reads remain permitted when the server is unavailable or unconfigured.**

This rule applies to EVERY request that involves this codebase.

### Rules

1. **Call `list_projects` FIRST** to discover the correct project name before using any tool.
2. If the checkout is not indexed, call `index_repository` first. Once a project is available, **call `mcp_codebase-memo_get_architecture` next** — before writing code, editing files, or answering any question about the codebase.
3. Use the returned context to make targeted, accurate changes.
4. **Do NOT use** `grep_search`, `file_search`, `semantic_search`, or `read_file` for initial codebase exploration when the graph tools are available and index covers the requested content.
5. Re-query only if additional context is needed during implementation.

Always use the project identifier returned by `list_projects` instead of guessing project names.

### Workflow

```
// Step 0 — discover available projects (do this first when MCP is available)
mcp_codebase-memo_list_projects()

// Step 0b — index repository first if checkout is not indexed
mcp_codebase-memo_index_repository({ "repo_path": "<repo_path>" })

// Step 1 — use the project identifier returned above once available
mcp_codebase-memo_get_architecture({ "project": "<display_name>" })

// Step 2 — find symbols
mcp_codebase-memo_search_graph({ "project": "<display_name>", "name_pattern": "<symbol>" })

// Step 3 — read code
mcp_codebase-memo_get_code_snippet({ "project": "<display_name>", "qualified_name": "<fn>" })
```

### Why

- Pre-built index covers the entire codebase with relevance ranking.
- Faster and more accurate than manual file search.
- Prevents reading stale files or following ghost references.
- Using `list_projects` avoids guessing project identifiers.
