import Link from "next/link";

export default function NotFound() {
  return (
    <div className="flex flex-col gap-4 justify-center items-center w-screen h-screen bg-gray-950 text-white">
      <h1 className="text-3xl font-bold">Paste not found</h1>
      <p className="text-gray-400">
        The paste you&apos;re looking for doesn&apos;t exist or may have been
        deleted.
      </p>
      <Link href="/" className="text-blue-400 hover:underline">
        Create a new paste
      </Link>
    </div>
  );
}
