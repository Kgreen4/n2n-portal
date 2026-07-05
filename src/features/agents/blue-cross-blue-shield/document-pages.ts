import { PDFDocument } from 'pdf-lib';
import { Buffer } from 'node:buffer';
import type { AgentDocument, AgentRunInput } from '../../../types/agent';

const PDF_MIME = 'application/pdf';

const bytesFromBase64 = (base64: string) => {
  return Uint8Array.from(Buffer.from(base64, 'base64'));
};

const base64FromBytes = (bytes: Uint8Array) => {
  return Buffer.from(bytes).toString('base64');
};

const pageName = (document: AgentDocument, pageNumber: number, pageCount: number) => {
  return `${document.name.replace(/\.pdf$/i, '')}-page-${String(pageNumber).padStart(3, '0')}-of-${String(pageCount).padStart(3, '0')}.pdf`;
};

const splitPdfDocument = async (document: AgentDocument): Promise<AgentDocument[]> => {
  const sourcePdf = await PDFDocument.load(bytesFromBase64(document.base64));
  const pageCount = sourcePdf.getPageCount();

  if (pageCount <= 1) {
    return [
      {
        ...document,
        pageNumber: 1,
        pageCount,
        sourceDocumentId: document.id,
        sourceDocumentName: document.name
      }
    ];
  }

  return await Promise.all(
    Array.from({ length: pageCount }, async (_, index) => {
      const pageNumber = index + 1;
      const pagePdf = await PDFDocument.create();
      const [copiedPage] = await pagePdf.copyPages(sourcePdf, [index]);
      pagePdf.addPage(copiedPage);
      const bytes = await pagePdf.save();

      return {
        ...document,
        id: `${document.id}:page:${pageNumber}`,
        name: pageName(document, pageNumber, pageCount),
        type: PDF_MIME,
        extension: '.pdf',
        size: bytes.byteLength,
        base64: base64FromBytes(bytes),
        pageNumber,
        pageCount,
        sourceDocumentId: document.id,
        sourceDocumentName: document.name
      };
    })
  );
};

export const explodeDocumentsByPage = async (input: AgentRunInput): Promise<AgentDocument[]> => {
  const pages = await Promise.all(
    input.documents.map((document) => {
      if (document.type !== PDF_MIME) {
        return Promise.resolve([document]);
      }

      return splitPdfDocument(document);
    })
  );

  return pages.flat();
};

export const inputForDocumentPage = (input: AgentRunInput, document: AgentDocument): AgentRunInput => ({
  ...input,
  documents: [document],
  notes: [
    input.notes,
    document.pageNumber && document.pageCount
      ? `Processing page ${document.pageNumber} of ${document.pageCount} from ${document.sourceDocumentName || document.name}.`
      : null
  ]
    .filter(Boolean)
    .join('\n')
});
