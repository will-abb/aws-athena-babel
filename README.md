# aws-athena-babel

**Run AWS Athena queries directly from Org Babel in Emacs.**

`aws-athena-babel` is an Emacs package that allows you to write and execute AWS Athena SQL queries from Org-mode source blocks using Org Babel. It leverages the AWS CLI to submit queries, monitors their status in real time, and displays results in both CSV and JSON formats.

[See a video demonstration](https://youtu.be/2VoVpH3ceG0)

## Features

- Submit Athena queries via AWS CLI directly from Org Babel blocks
- Asynchronous query monitoring in a dedicated `*Athena Monitor*` buffer
- Live status updates with execution metrics and cost estimation
- Console-style result rendering in Org-mode tables
- JSON conversion from CSV output
- CSV and JSON views with dedicated keybindings
- Supports result reuse with Athena Workgroups
- Displays full Athena Console URLs for easy web access
- Provides local raw result file (`/tmp/<query-id>.csv`) for inspection or scripting
- Displays a direct AWS Console link to the query execution
- Default CSV output directory is the system temporary directory (as returned by `temporary-file-directory`)
- Keybindings for quick actions:
  - `C-c C-k`: Cancel running query
  - `C-c C-c`: Show raw CSV output
  - `C-c C-j`: Show JSON output (for CloudTrail buckets)
  - `C-c C-a`: Open Athena Console link in browser
  - `C-c C-l`: Open local CSV result in Emacs

## Requirements

- Emacs ≥ 26.1
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- Properly configured AWS credentials (via profile or environment)
- Athena output location configured in S3

## Installation

### Doom Emacs

Add the following to your Doom Emacs `packages.el`:

```emacs-lisp
(package! aws-athena-babel
  :recipe (:host github :repo "will-abb/aws-athena-babel"))
```

Then in your `config.el`:

```emacs-lisp
(after! org
  (add-to-list 'org-src-lang-modes '("athena" . sql)))
```

Then same as below but `use-package!`

### Regular Emacs

If you're using `use-package` in your Emacs config, you can load `aws-athena-babel` like this:

```emacs-lisp
(use-package aws-athena-babel
  :load-path "~/.emacs.d/aws-athena-babel"
  :commands (org-babel-execute:athena)
  :config
  (setq aws-athena-babel-output-location "s3://your-bucket/path/"
        aws-athena-babel-workgroup "primary"
        aws-athena-babel-profile "default"
        aws-athena-babel-database "default"
        aws-athena-babel-poll-interval 3
        aws-athena-babel-fullscreen-monitor-buffer t
        aws-athena-babel-result-reuse-enabled t
        aws-athena-babel-result-reuse-max-age 10080
        aws-athena-babel-console-region "us-east-1"
        aws-athena-babel-csv-output-dir "/my-result-directory"))
```

## Usage

1. In an Org-mode buffer, insert a source block:

    ```org
    #+begin_src athena
    SELECT * FROM your_table LIMIT 10;
    #+end_src
    ```

2. Run the block using `C-c C-c` inside the block.

3. Monitor progress in `*Athena Monitor*`, view CSV or JSON with keybindings.

4. Results are by default saved to `/tmp/<query-id>.csv` and also rendered in Org-mode table format.

## Output Rendering

- **Org Table**: Console-style format based on Athena's CSV output.
- **CSV**: Raw download from S3, shown in a dedicated buffer.
- **JSON**: Parsed and cleaned from CSV into structured objects.
- **Local CSV**: Saved to system temp dir and openable with `C-c C-l`
- **Console Link**: Openable in browser via `C-c C-a`

*The queries and the outputs have solely been tested with CloudTrail buckets.*

## License

GPL-3.0-or-later © 2025 [Williams Bosch-Bello](mailto:williamsbosch@gmail.com)

## Contributions

Issues and pull requests are welcome on [GitHub](https://github.com/will-abb/aws-athena-babel).
