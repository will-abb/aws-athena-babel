# ob-athena

**Run AWS Athena queries directly from Org Babel in Emacs.**

`ob-athena` is an Emacs package that allows you to write and execute AWS Athena SQL queries from Org-mode source blocks using Org Babel. It leverages the AWS CLI to submit queries, monitors their status in real time, and displays results in both CSV and JSON formats.

[See a video demonstration](https://youtu.be/2VoVpH3ceG0)

## Features

- Submit Athena queries via AWS CLI directly from Org Babel blocks
- Asynchronous query monitoring in a dedicated `*Athena Monitor*` buffer
- Live status updates with execution metrics and cost estimation
- Console-style result rendering in Org-mode tables
- JSON conversion from CSV output using the Miller (`mlr`) tool
- CSV and JSON views with dedicated keybindings
- Supports result reuse with Athena Workgroups
- Displays full Athena Console URLs for easy web access
- Provides local raw result file (`/tmp/<query-id>.csv`) for inspection or scripting
- Accepts full Org Babel header arguments:
  - `:aws-profile`, `:database`, `:s3-output-location`, `:workgroup`, etc.
  - Also supports execution configuration like `:poll-interval`, `:fullscreen`, etc.
- Automatically replaces `${var}` placeholders in queries using `:var` bindings
- Keybindings for quick actions:
  - `C-c C-k`: Cancel running query
  - `C-c C-c`: Show raw CSV output
  - `C-c C-j`: Show JSON output (requires Miller `mlr`)
  - `C-c C-a`: Open Athena Console link in browser
  - `C-c C-l`: Open local CSV result in Emacs

## Requirements

- Emacs ≥ 26.1
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- Properly configured AWS credentials (via profile or environment)
- Athena output location configured in S3
- If you plan to use the json formater, install [Miller Tool](https://github.com/johnkerl/miller)

## Installation

### Doom Emacs

Add the following to your Doom Emacs `packages.el`:

```emacs-lisp
(package! ob-athena
  :recipe (:host github :repo "will-abb/aws-athena-babel"))
```

Then in your `config.el`:

```emacs-lisp
(after! org
  (add-to-list 'org-src-lang-modes '("athena" . sql)))
```

Then same as below but `use-package!`

### Regular Emacs

If you're using `use-package` in your Emacs config, you can load `ob-athena` like this:

```emacs-lisp
(use-package ob-athena
  :load-path "~/.emacs.d/ob-athena"
  :commands (org-babel-execute:athena)
  :config
  (setq ob-athena-s3-output-location "s3://your-bucket/path/"
        ob-athena-workgroup "primary"
        ob-athena-profile "default"
        ob-athena-database "default"
        ob-athena-poll-interval 3
        ob-athena-fullscreen-monitor-buffer t
        ob-athena-result-reuse-enabled t
        ob-athena-result-reuse-max-age 10080
        ob-athena-console-region "us-east-1"
        ob-athena-csv-output-dir "/my-result-directory"))
```

*With support for source block headers arguments in v2.0.0 and up you can set variables there instead, allowing per query settings.*

## Usage

1. In an Org-mode buffer, insert a source block with header arguments on the same line:
   ```org
   #+begin_src athena :aws-profile "default" :database "yourdb" :s3-output-location "s3://your-bucket/path/" :workgroup "primary" :poll-interval 3 :fullscreen t :result-reuse-enabled t :result-reuse-max-age 10080 :console-region "us-east-1" :var user="john.doe@example.com"
   SELECT * FROM your_table WHERE user = '${user}' LIMIT 10;
   #+end_src
   ```
2. Run the block using `C-c C-c` inside the source block
3. Query progress and metrics appear in the `*Athena Monitor*` buffer
4. Press:
   * `C-c C-c` to view raw CSV.
   * `C-c C-j` to view JSON output (parsed using `mlr`)
   * `C-c C-l` to open the local CSV result file
   * `C-c C-a` to open the Athena Console in your browser
5. Results are saved by default to `/tmp/<query-id>.csv`
## Output Rendering

- **Org Table**: Console-style format based on Athena's CSV output
- **CSV**: Raw download from S3, shown in a dedicated buffer
- **JSON**: Automatically converted using the `mlr` tool (`mlr --icsv --ojson`)
- **Local CSV**: Saved to system temp dir and openable with `C-c C-l`
- **Console Link**: Openable in browser via `C-c C-a`

*The queries and the outputs have mostly been tested with CloudTrail buckets.*

## License

GPL-3.0-or-later © 2025 [Williams Bosch-Bello](mailto:williamsbosch@gmail.com)

## Contributions

Issues and pull requests are welcome on [GitHub](https://github.com/will-abb/aws-athena-babel).
