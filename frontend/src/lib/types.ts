export type StepStatus = 'idle' | 'running' | 'done' | 'error';

export interface PipelineEvent {
  type:
    | 'tool_call'
    | 'tool_result'
    | 'thought'
    | 'error'
    | 'done'
    | 'stopped'
    | 'result'
    | 'script_path';
  // For regular events:
  content?: string;
  // For result events:
  success?: boolean;
  files?: string[];
  previews?: Record<string, string>;
  error?: string;
  // For script_path events:
  path?: string;
}

export interface FileInfo {
  name: string;
  size: number;
  path: string;
  ext: string;
}

export interface PreviewData {
  type: 'csv' | 'text' | 'error';
  columns?: string[];
  rows?: (string | number | null)[][];
  shape?: [number, number];
  dtypes?: Record<string, string>;
  content?: string;
  message?: string;
}
