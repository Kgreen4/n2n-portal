import { MIME, type MimeType, type MediaExtension } from '../../types/media';

export type MediaStatus = 'ready' | 'rejected';

export type MediaFile = {
  id: string;
  originalName: string;
  name: string;
  type: MimeType | string;
  extension: MediaExtension | string;
  size: number;
  base64: string;
  dataUrl: string;
  status: MediaStatus;
  error?: string;
};

type UseMediaOptions = {
  types?: readonly string[];
  extensions?: readonly string[];
  maxBytes?: number;
};

const defaultOptions = {
  types: [MIME.documents.types.pdf],
  extensions: [MIME.documents.extentions.pdf],
  maxBytes: 25 * 1024 * 1024
} as const;

export const useMedia = (options: UseMediaOptions = {}) => {
  const allowedTypes = options.types ?? defaultOptions.types;
  const allowedExtensions = options.extensions ?? defaultOptions.extensions;
  const maxBytes = options.maxBytes ?? defaultOptions.maxBytes;
  let files = $state<MediaFile[]>([]);

  const sanitizeName = (name: string) => {
    const normalized = name.normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
    const stripped = normalized
      .replace(/[\\/]/g, '-')
      .replace(/[\x00-\x1f\x7f]/g, '')
      .replace(/[^a-zA-Z0-9._ -]/g, '')
      .replace(/\s+/g, ' ')
      .trim();

    return stripped || 'unnamed-file';
  };

  const extensionOf = (name: string) => {
    const index = name.lastIndexOf('.');
    return index >= 0 ? name.slice(index).toLowerCase() : '';
  };

  const createId = (file: File) => {
    return `${file.name}-${file.size}-${file.lastModified}-${crypto.randomUUID()}`;
  };

  const rejectFile = (file: File, error: string): MediaFile => {
    const name = sanitizeName(file.name);

    return {
      id: createId(file),
      originalName: file.name,
      name,
      type: file.type || 'unknown',
      extension: extensionOf(name),
      size: file.size,
      base64: '',
      dataUrl: '',
      status: 'rejected',
      error
    };
  };

  const hasAllowedType = (file: File) => {
    return allowedTypes.includes(file.type);
  };

  const hasAllowedExtension = (name: string) => {
    return allowedExtensions.includes(extensionOf(name));
  };

  const hasPdfSignature = async (file: File) => {
    const header = new Uint8Array(await file.slice(0, 5).arrayBuffer());
    const signature = Array.from(header)
      .map((byte) => String.fromCharCode(byte))
      .join('');

    return signature === '%PDF-';
  };

  const readAsDataUrl = (file: File) => {
    return new Promise<string>((resolve, reject) => {
      const reader = new FileReader();

      reader.onload = () => resolve(String(reader.result ?? ''));
      reader.onerror = () => reject(reader.error ?? new Error('media: unable to read file'));
      reader.readAsDataURL(file);
    });
  };

  const toMediaFile = async (file: File): Promise<MediaFile> => {
    const name = sanitizeName(file.name);

    if (!hasAllowedType(file)) {
      return rejectFile(file, 'Unsupported file type');
    }

    if (!hasAllowedExtension(name)) {
      return rejectFile(file, 'Unsupported file extension');
    }

    if (file.size > maxBytes) {
      return rejectFile(file, 'File is too large');
    }

    if (!(await hasPdfSignature(file))) {
      return rejectFile(file, 'File did not pass PDF integrity check');
    }

    const dataUrl = await readAsDataUrl(file);
    const base64 = dataUrl.split(',')[1] ?? '';

    if (!base64) {
      return rejectFile(file, 'Unable to encode file');
    }

    return {
      id: createId(file),
      originalName: file.name,
      name,
      type: file.type,
      extension: extensionOf(name),
      size: file.size,
      base64,
      dataUrl,
      status: 'ready'
    };
  };

  const addFiles = async (nextFiles: FileList | File[]) => {
    const incoming = Array.from(nextFiles);
    const parsed = await Promise.all(incoming.map(toMediaFile));
    files = [...files, ...parsed];
  };

  const removeFile = (id: string) => {
    files = files.filter((file) => file.id !== id);
  };

  const clear = () => {
    files = [];
  };

  const formatBytes = (bytes: number) => {
    if (bytes === 0) {
      return '0 B';
    }

    const units = ['B', 'KB', 'MB', 'GB'] as const;
    const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const value = bytes / 1024 ** index;

    return `${value.toFixed(value >= 10 || index === 0 ? 0 : 1)} ${units[index]}`;
  };

  return {
    get files() {
      return files;
    },
    addFiles,
    removeFile,
    clear,
    formatBytes
  };
};
