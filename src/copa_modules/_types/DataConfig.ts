/**
 * I am using this file to write the types, since I am more comfortable with typescript.
 * I am converting this to python types using chatGPT.
 * https://chat.openai.com/share/4e0bf20a-d266-4e39-8ed2-7ebfbc233598
 */

interface FileConfig {
  fileName: string;
  fileType: "csv"; // for now we only have csv files.
  colOfInterest: string[];
  separator?: "," | ";";
  filePathOverwrite?: string; // This will overwrite the path which is generated using base path and filename.
}

type DataConfig = FileConfig[];