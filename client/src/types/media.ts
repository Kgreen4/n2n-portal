const DOCUMENT_TYPES = {
  pdf: 'application/pdf',
  csv: 'text/csv',
  plainText: 'text/plain',
  json: 'application/json',
  xml: 'application/xml',
  html: 'text/html',
  markdown: 'text/markdown'
} as const;

const DOCUMENT_EXTENSIONS = {
  pdf: '.pdf',
  csv: '.csv',
  plainText: '.txt',
  json: '.json',
  xml: '.xml',
  html: '.html',
  markdown: '.md'
} as const;

const IMAGE_TYPES = {
  png: 'image/png',
  jpeg: 'image/jpeg',
  tiff: 'image/tiff',
  gif: 'image/gif',
  webp: 'image/webp',
  svg: 'image/svg+xml',
  bmp: 'image/bmp'
} as const;

const IMAGE_EXTENSIONS = {
  png: '.png',
  jpg: '.jpg',
  jpeg: '.jpeg',
  tif: '.tif',
  tiff: '.tiff',
  gif: '.gif',
  webp: '.webp',
  svg: '.svg',
  bmp: '.bmp'
} as const;

export const MIME = {
  documents: {
    types: DOCUMENT_TYPES,
    extentions: DOCUMENT_EXTENSIONS,
    extensions: DOCUMENT_EXTENSIONS,
    accept: [
      DOCUMENT_TYPES.pdf,
      DOCUMENT_TYPES.csv,
      IMAGE_TYPES.png,
      IMAGE_TYPES.jpeg,
      IMAGE_TYPES.tiff,
      DOCUMENT_EXTENSIONS.pdf,
      DOCUMENT_EXTENSIONS.csv,
      IMAGE_EXTENSIONS.png,
      IMAGE_EXTENSIONS.jpg,
      IMAGE_EXTENSIONS.jpeg,
      IMAGE_EXTENSIONS.tif,
      IMAGE_EXTENSIONS.tiff
    ].join(',')
  },
  images: {
    types: IMAGE_TYPES,
    extensions: IMAGE_EXTENSIONS
  },
  audio: {
    types: {
      mpeg: 'audio/mpeg',
      wav: 'audio/wav',
      ogg: 'audio/ogg'
    },
    extensions: {
      mp3: '.mp3',
      wav: '.wav',
      ogg: '.ogg'
    }
  },
  video: {
    types: {
      mp4: 'video/mp4',
      mpeg: 'video/mpeg',
      quicktime: 'video/quicktime',
      webm: 'video/webm'
    },
    extensions: {
      mp4: '.mp4',
      mpeg: '.mpeg',
      mov: '.mov',
      webm: '.webm'
    }
  },
  archives: {
    types: {
      zip: 'application/zip',
      octetStream: 'application/octet-stream'
    },
    extensions: {
      zip: '.zip'
    }
  }
} as const;

export type MediaGroup = keyof typeof MIME;

export type MimeType = {
  [Group in MediaGroup]: (typeof MIME)[Group]['types'][keyof (typeof MIME)[Group]['types']];
}[MediaGroup];

export type MediaExtension = {
  [Group in MediaGroup]: (typeof MIME)[Group]['extensions'][keyof (typeof MIME)[Group]['extensions']];
}[MediaGroup];

export type DocumentMimeType = (typeof MIME.documents.types)[keyof typeof MIME.documents.types];
export type DocumentExtention = (typeof MIME.documents.extentions)[keyof typeof MIME.documents.extentions];
export type DocumentExtension = DocumentExtention;
