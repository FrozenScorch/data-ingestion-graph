/**
 * Node type registry for Svelte Flow.
 * Maps all node types to the generic PipelineNode component.
 * Each node type is distinguished by its data.category and data.icon fields.
 */

import PipelineNode from './PipelineNode.svelte';
import type { NodeTypes } from '@xyflow/svelte';

// All node types use the same PipelineNode component with different data
const nodeTypeList = [
  'file_source',
  'database_source',
  'webhook_source',
  'discord_source',
  'github_source',
  'sec_edgar_source',
  'file_parser',
  'text_chunker',
  'html_splitter',
  'recursive_splitter',
  'semantic_chunker',
  'filter',
  'transform',
  'split',
  'merge',
  'embedder',
  'llm_extract',
  'llm_classify',
  'llm_summarize',
  'vector_store',
  'database_writer',
  'http_request'
];

export const nodeTypes: NodeTypes = Object.fromEntries(
  nodeTypeList.map(type => [type, PipelineNode])
);
