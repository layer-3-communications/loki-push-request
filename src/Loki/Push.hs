{-# language DuplicateRecordFields #-}
{-# language OverloadedStrings #-}
{-# language PatternSynonyms #-}

module Loki.Push
  ( PushRequest(..)
  , Stream(..)
  , Entry(..)
  , LabelPair(..)
  , pushRequestToJson
  ) where

import Json (pattern (:->))
import Data.Text.Short (ShortText)
import Data.Int (Int64)
import Data.Primitive (SmallArray,ByteArray(ByteArray))
import Data.ByteString.Short.Internal (ShortByteString(SBS))

import qualified Json
import qualified Data.Primitive.Contiguous as Contiguous
import qualified Data.Bytes.Builder.Bounded as Build
import qualified Data.Text.Short.Unsafe as TSU
import qualified Arithmetic.Nat as Nat

newtype PushRequest = PushRequest { streams :: SmallArray Stream }

data Stream = Stream
  { labels :: !(SmallArray LabelPair)
  , entries :: !(SmallArray Entry)
  }

data LabelPair = LabelPair
  { name :: !ShortText
  , value :: !ShortText
  }

data Entry = Entry
  { timestamp :: !Int64
  , line :: !ShortText
  , metadata :: !(SmallArray LabelPair)
  }

-- Example JSON encoding from loki docs:
--
-- > {
-- >   "streams": [
-- >     {
-- >       "stream": {
-- >         "foo": "bar2"
-- >       },
-- >       "values": [
-- >         [
-- >           "1570818238000000000",
-- >           "fizzbuzz"
-- >         ]
-- >       ]
-- >     }
-- >   ]
-- > }
--
-- Strangely, this does not match what is suggested by the gogoproto
-- files that the docs link to.


pushRequestToJson :: PushRequest -> Json.Value
pushRequestToJson (PushRequest streams) = Json.object1
  ("streams" :-> Json.Array (fmap streamToJson streams)
  )
  
streamToJson :: Stream -> Json.Value
streamToJson Stream{labels,entries} = Json.object2
  ("stream" :-> Json.Object (fmap labelPairToMember labels))
  ("values" :-> Json.Array (fmap entryToJson entries))

entryToJson :: Entry -> Json.Value
entryToJson Entry{timestamp,line,metadata} = Json.Array $ Contiguous.tripleton
  ( case Build.run Nat.constant (Build.int64Dec timestamp) of
      ByteArray x -> Json.String (TSU.fromShortByteStringUnsafe (SBS x))
  )
  (Json.String line)
  (Json.Object (fmap labelPairToMember metadata))

labelPairToMember :: LabelPair -> Json.Member
labelPairToMember LabelPair{name,value} =
  Json.Member{key = name, value = Json.String value}
